#!/usr/bin/env python3
"""Tempestas Raspberry Pi weather station.

Current hardware is discovered by weather_sensor_test.py and stored in
~/weather_hardware.json.  The station reads that file at startup instead of
hard-coding GPIO assignments.  It supports multiple DHT22 / AM2302 sensors,
an optional BME280 on I2C address 0x76 or 0x77, and the Raspberry Pi camera.

All discovered DHT22 sensors are retained as independent inputs.  Each DHT is
sampled continuously every ~2.1 s into a five-attempt rolling buffer (failed
attempts are stored as NaN).  Logical 5-s DHT samples are reconstructed at a
2.1-s look-back using one-point passthrough, two-point linear interpolation, or
shape-preserving PCHIP interpolation for three or more valid observations.
Those logical samples are then block-medianed and combined into a DHT ensemble
value.  When a BME280 is present, its readings remain independent and the
Combined_* channels compare/average the DHT ensemble with the BME280.  Ambient_*
uses the DHT ensemble first and falls back to BME280 when no DHT reading is
available.

Design goals:
  * one failed sensor must not terminate the station
  * never poll the DHT22 faster than it should be polled
  * camera/network work must not trigger extra environmental-sensor reads
  * preserve useful diagnostic output under systemd
  * never silently invent replacement sensor values
  * retain a bounded local resend window without risking filesystem exhaustion
"""

import csv
import functools
import glob
import json
import math
import os
import queue
import shutil
import statistics
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone

import psutil

import board

try:
    import adafruit_dht
except Exception as exc:
    adafruit_dht = None
    DHT_IMPORT_ERROR = exc
else:
    DHT_IMPORT_ERROR = None

try:
    import adafruit_bme280.basic as adafruit_bme280
except Exception as exc:
    adafruit_bme280 = None
    BME280_IMPORT_ERROR = exc
else:
    BME280_IMPORT_ERROR = None

try:
    from picamera2 import Picamera2
    import libcamera
except Exception as exc:
    Picamera2 = None
    libcamera = None
    CAMERA_IMPORT_ERROR = exc
else:
    CAMERA_IMPORT_ERROR = None

try:
    from PIL import Image
except Exception as exc:
    Image = None
    PIL_IMPORT_ERROR = exc
else:
    PIL_IMPORT_ERROR = None


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

BASE_DIR = "/home/njm"
IMAGE_DIR = os.path.join(BASE_DIR, "images")
LOCAL_WEATHER_CSV = os.path.join(BASE_DIR, "weather_data.csv")
LOCAL_SYSTEM_CSV = os.path.join(BASE_DIR, "system_usage.csv")
HARDWARE_CONFIG_PATH = os.path.join(BASE_DIR, "weather_hardware.json")
CONNECTIVITY_STATE_PATH = os.path.join(BASE_DIR, "weather_connectivity_state.json")

SERVER_ADDRESS = "nill@nillmill.ddns.net"
SERVER_BASE = "/media/data/weather_station"
SERVER_IMAGE_DIR = os.path.join(SERVER_BASE, "images/")
SERVER_WEATHER_CSV = os.path.join(SERVER_BASE, "weather_data.csv")
SERVER_SYSTEM_CSV = os.path.join(SERVER_BASE, "system_usage.csv")

# Sensor assignments are populated from HARDWARE_CONFIG_PATH.  These defaults
# preserve the pre-discovery setup only when the config file does not yet exist.
DEFAULT_DHT_GPIOS = [4]
DEFAULT_DHT_POWER_GPIO = 17       # physical pin 11: switched 3.3-V rail for DHTs
DEFAULT_DHT_POWER_PHYSICAL_PIN = 11
DEFAULT_BME280_I2C_ADDRESS = 0x77

# DHT22 / AM2302 acquisition has two layers:
#
#   physical sampler:
#       each sensor is polled continuously in its own background worker at the
#       DHT22-safe cadence (~2.1 s). Every attempt enters a five-attempt rolling
#       buffer. Failed attempts enter as NaN rather than leaving old data behind.
#
#   logical sampler:
#       the existing weather-station scheduler still asks each DHT reader for
#       one value every 5 s. read_once() does NOT touch the hardware. Instead it
#       estimates the value at (request time - 2.1 s) from the rolling physical
#       samples. This normally places the target between real observations, so
#       the result is interpolation rather than extrapolation.
#
# With one valid point, that point is returned. With two valid points, linear
# interpolation is used. With three or more valid points, a shape-preserving
# PCHIP interpolation is used. If the target is not bracketed by valid samples,
# the nearest valid observation is returned rather than extrapolating.
DHT_POLL_INTERVAL_S = 5.0
DHT_PHYSICAL_POLL_INTERVAL_S = 2.1
DHT_INTERPOLATION_LOOKBACK_S = 2.1
DHT_ROLLING_ATTEMPTS = 5
DHT_SENSOR_START_STAGGER_S = 0.10

# The DHT library bit-bangs timing-critical GPIO transactions in user space.
# Only one physical DHT transaction is allowed to run at a time, even though
# each sensor has its own background worker. Workers are staggered so this lock
# is normally uncontended.
DHT_PHYSICAL_IO_LOCK = threading.Lock()

DHT_REINITIALIZE_AFTER_FAILURES = 12
DHT_SAMPLE_BUFFER_SIZE = 24

# GPIO17 / physical pin 11 now powers both DHT sensors directly.  A real
# power-cycle is therefore possible without rebooting the Raspberry Pi.  The
# station only performs one automatically when every configured DHT has a full
# five-attempt rolling buffer containing zero valid measurements for multiple
# logical checks.  A cooldown prevents repeated cycling if there is a genuine
# wiring/sensor fault rather than the observed DHT latch-up.
DHT_POWER_OFF_S = 5.0
DHT_POWER_STARTUP_S = 3.0
DHT_POWER_RESET_CONFIRMATIONS = 2
DHT_POWER_RESET_COOLDOWN_S = 60.0

# BME280 is optional.  weather_sensor_test.py writes null when no BME280 is
# detected; the station then leaves the I2C reader disabled until discovery is
# run again.
BME280_POLL_INTERVAL_S = 5.0
BME280_REINITIALIZE_AFTER_FAILURES = 6
BME280_SAMPLE_BUFFER_SIZE = 24
# Altitude is derived from pressure, not a direct sensor measurement.  Keep the
# standard sea-level reference explicit so the stored value is reproducible.
BME280_SEA_LEVEL_PRESSURE_HPA = 1013.25

DATA_LOG_INTERVAL_S = 30.0
UPLOAD_INTERVAL_S = 300.0
# The server is the permanent archive.  The Pi retains a rolling resend window,
# not an unbounded second archive. After a confirmed upload, compact to seven
# days at normal cadence. During an outage, adaptive sampling greatly extends
# that span; an absolute row ceiling remains as a final software guard.
LOCAL_CSV_RETAIN_AFTER_UPLOAD_DAYS = 7
LOCAL_CSV_RETENTION_CHECK_INTERVAL_S = 3600.0
LOCAL_CSV_ABSOLUTE_MAX_ROWS = 250_000

# Simple store-and-forward policy.  An outage progressively trades temporal
# resolution for endurance.  The state is persisted, so a reboot without Wi-Fi
# does not reset the station to its most storage-intensive mode.
OFFLINE_POLICIES = (
    # minimum outage, telemetry interval, image interval, reconnect probe
    (0,          30.0,      300.0,      300.0,  "normal-first-day"),
    (24 * 3600,  300.0,     3600.0,     900.0,  "hourly-images-first-week"),
    (7 * 86400,  900.0,     6 * 3600,   1800.0, "six-hour-images-first-month"),
    (30 * 86400, 3600.0,    24 * 3600,  3600.0, "daily-images-long-term"),
)
IMAGE_BACKLOG_UPLOADS_PER_CYCLE = 2
# Last-resort rolling mode.  Oldest local images are removed only as needed to
# preserve this much working space; sparse CSV telemetry continues.
MIN_FREE_DISK_GB = 5.0
MIN_FREE_DISK_PERCENT = 5.0

# Camera scene classification uses hysteresis so dusk/dawn does not cause the
# camera to flap between profiles every five minutes.  Lux is the primary
# signal; metered ExposureTime * AnalogueGain is used if Lux is unavailable.
CAMERA_DAY_ENTER_LUX = 35.0
CAMERA_DAY_EXIT_LUX = 20.0
CAMERA_NIGHT_ENTER_LUX = 4.0
CAMERA_NIGHT_EXIT_LUX = 8.0

# Auto-metering is performed at the start of every capture.  Picamera2 reports
# exposure/gain/lux in metadata for completed frames, so no image array needs to
# be copied merely to let AE/AWB settle.
CAMERA_METER_MAX_FRAMES = 10
CAMERA_METER_REQUIRED_STABLE = 2
CAMERA_STABLE_THRESHOLD = 0.03

# The IMX477 is intentionally operated with its physical IR-cut filter out so
# nighttime near-IR sensitivity is preserved. Daytime therefore uses the fixed
# NoIR colour calibration tuned interactively with rpicam-vid.
#
# These are ISP/libcamera controls, not a post-JPEG/Pillow colour transform.
# Metering still begins with AE/AWB enabled; after classification as DAY, AWB is
# disabled and these calibrated controls settle before the final JPEG capture.
CAMERA_METER_SATURATION = 1.0
CAMERA_DAY_NOIR_RED_GAIN = 0.90
CAMERA_DAY_NOIR_BLUE_GAIN = 1.30
CAMERA_DAY_NOIR_SATURATION = 1.40
CAMERA_DAY_NOIR_CCM = (
     1.35, -0.25, -0.10,
    -0.15,  1.30, -0.15,
    -0.10, -0.25,  1.35,
)
CAMERA_DAY_CONTROL_SETTLE_FRAMES = 3

# Twilight deliberately remains strongly desaturated because near-IR becomes
# increasingly dominant and faithful colour reconstruction is unreliable.
CAMERA_TWILIGHT_SATURATION = 0.20
CAMERA_TWILIGHT_EXPOSURE_VALUE = 0.5

# Night profile: fixed manual exposure/gain, monochrome, and frame stacking.
# The target is chosen from the auto-metered scene brightness, then clamped to
# whatever ranges this particular camera mode advertises at runtime.
CAMERA_NIGHT_MAX_EXPOSURE_US = 4_000_000
CAMERA_NIGHT_MAX_ANALOGUE_GAIN = 12.0
CAMERA_NIGHT_FRAME_MARGIN_US = 100_000
CAMERA_NIGHT_MANUAL_SETTLE_FRAMES = 3
CAMERA_NIGHT_STACK_SHORT = 4     # exposure <= 1 s
CAMERA_NIGHT_STACK_LONG = 3      # exposure > 1 s
CAMERA_NIGHT_SHORT_EXPOSURE_US = 1_000_000

# (minimum metered lux, target exposure microseconds, target analogue gain)
CAMERA_NIGHT_PROFILES = (
    (2.0, 150_000, 3.0),
    (1.0, 350_000, 4.0),
    (0.2, 800_000, 6.0),
    (0.05, 1_500_000, 8.0),
    (-math.inf, 3_000_000, 10.0),
)

# Rotation is still performed in libcamera. Pillow remains a dependency only
# for NIGHT frame stacking; DAY colour correction now occurs entirely in the
# camera ISP so there is exactly one daylight correction stage.
CAMERA_ROTATE_180 = True
CAMERA_JPEG_QUALITY = 95

# Uploads use rsync only: the server receives into a hidden temporary file and
# renames it into place, so readers never see a half-written CSV or JPEG, and
# the delta transfer sends little more than the rows added since the last
# upload.  --bwlimit is KiB/s; 62 KiB/s matches the former scp -l 500 kbit/s.
RSYNC_MAX_RETRIES = 3
RSYNC_RETRY_DELAY_S = 5.0
RSYNC_BANDWIDTH_LIMIT_KIB_S = "62"
RSYNC_CONNECT_TIMEOUT_S = "10"
RSYNC_PROCESS_TIMEOUT_S = 180
# CSV snapshots for upload live in RAM (tmpfs) to avoid extra SD-card writes.
UPLOAD_SNAPSHOT_DIR = "/dev/shm/weather_station_upload"
UPLOAD_STOP_TIMEOUT_S = 10.0

# Warn when the independent sensors disagree substantially.  Warnings do not
# alter either raw reading or the combined value.
TEMPERATURE_DISAGREEMENT_WARN_C = 2.0
HUMIDITY_DISAGREEMENT_WARN_PERCENT = 10.0


def load_hardware_config(path=HARDWARE_CONFIG_PATH):
    """Load the hardware map written by weather_sensor_test.py.

    If discovery has never been run, retain the previous single-DHT/BME280
    defaults. If discovery *has* run, an explicitly absent BME280 remains
    disabled instead of being repeatedly probed.
    """
    fallback = {
        "version": 3,
        "dht22": [{"gpio": gpio} for gpio in DEFAULT_DHT_GPIOS],
        "dht_power": {
            "gpio": DEFAULT_DHT_POWER_GPIO,
            "physical_pin": DEFAULT_DHT_POWER_PHYSICAL_PIN,
        },
        "bme280": {"address": DEFAULT_BME280_I2C_ADDRESS},
    }

    if not os.path.exists(path):
        return fallback

    try:
        with open(path, "r", encoding="utf-8") as handle:
            config = json.load(handle)
        if not isinstance(config, dict):
            raise ValueError("hardware config root is not an object")
        return config
    except Exception as exc:
        # Logging is not defined yet at module-import time.
        print(
            f"WARNING: could not load {path}: {exc}; using legacy defaults",
            file=sys.stderr,
            flush=True,
        )
        return fallback


HARDWARE_CONFIG = load_hardware_config()

_dht_power_config = HARDWARE_CONFIG.get("dht_power")
if isinstance(_dht_power_config, dict) and _dht_power_config.get("gpio") is not None:
    try:
        DHT_POWER_GPIO = int(_dht_power_config["gpio"])
    except (TypeError, ValueError):
        DHT_POWER_GPIO = DEFAULT_DHT_POWER_GPIO
else:
    # Existing version-1 hardware maps predate the switched DHT supply.  The
    # physical wiring has now moved to GPIO17, so use that as the compatibility
    # default until the tester rewrites the config as version 2.
    DHT_POWER_GPIO = DEFAULT_DHT_POWER_GPIO

DHT_POWER_PHYSICAL_PIN = DEFAULT_DHT_POWER_PHYSICAL_PIN

DHT_GPIOS = []
for entry in HARDWARE_CONFIG.get("dht22", []):
    try:
        gpio = int(entry["gpio"] if isinstance(entry, dict) else entry)
    except (KeyError, TypeError, ValueError):
        continue
    if gpio == DHT_POWER_GPIO:
        print(
            f"WARNING: ignoring GPIO{gpio} as DHT data because it is the "
            "configured DHT power rail",
            file=sys.stderr,
            flush=True,
        )
        continue
    if gpio not in DHT_GPIOS:
        DHT_GPIOS.append(gpio)

_bme_config = HARDWARE_CONFIG.get("bme280")
if _bme_config is None:
    # Backward compatibility with the previous BME280-labelled hardware map.
    # Only the I2C address is reused; this station now initializes a BME280.
    _bme_config = HARDWARE_CONFIG.get("bme680")

if isinstance(_bme_config, dict) and _bme_config.get("address") is not None:
    try:
        BME280_I2C_ADDRESS = int(_bme_config["address"])
    except (TypeError, ValueError):
        BME280_I2C_ADDRESS = None
else:
    BME280_I2C_ADDRESS = None


# Preserve the historical aggregate DHT22 columns, then add one raw pair for
# every discovered DHT so downstream analysis can inspect the sensors
# independently.  A changed sensor count deliberately changes the CSV schema;
# backup_incompatible_csv() will preserve the previous local file.
_DHT_DETAIL_HEADER = []
for _index, _gpio in enumerate(DHT_GPIOS, start=1):
    _DHT_DETAIL_HEADER.extend(
        [
            f"DHT22_{_index}_GPIO",
            f"DHT22_{_index}_Temperature_C",
            f"DHT22_{_index}_Humidity_percent",
        ]
    )

WEATHER_HEADER = [
    "Timestamp",
    "Ambient_Temperature_C",
    "Ambient_Humidity_percent",
    "Combined_Temperature_C",
    "Combined_Humidity_percent",
    "DHT22_Temperature_C",
    "DHT22_Humidity_percent",
    *_DHT_DETAIL_HEADER,
    "BME280_Temperature_C",
    "BME280_Humidity_percent",
    "BME280_Pressure_hPa",
    "BME280_Altitude_m",
    "Camera_Lux",
    "Camera_Lux_Age_s",
]

SYSTEM_HEADER = [
    "Timestamp",
    "CPU_Temperature_C",
    "CPU_Usage_percent",
    "Memory_Usage_percent",
    "Disk_Free_GB",
    "Pi_Throttled_Hex",
]

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(line_buffering=True)


# -----------------------------------------------------------------------------
# Logging / generic utilities
# -----------------------------------------------------------------------------

def log(message, level="INFO"):
    timestamp = datetime.now().isoformat(timespec="seconds")
    print(f"{timestamp} [{level}] {message}", flush=True)


def safe_float(value, default=math.nan):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def is_finite(value):
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def median_or_nan(values):
    finite = [float(value) for value in values if is_finite(value)]
    if not finite:
        return math.nan
    return float(statistics.median(finite))


def mean_or_nan(values):
    finite = [float(value) for value in values if is_finite(value)]
    if not finite:
        return math.nan
    return float(statistics.fmean(finite))


# One lock serialises every write or rewrite of the local telemetry CSVs and
# the upload snapshot, so the background upload never copies a partial row.
CSV_LOCK = threading.RLock()


def with_csv_lock(function):
    @functools.wraps(function)
    def locked(*args, **kwargs):
        with CSV_LOCK:
            return function(*args, **kwargs)
    return locked


@with_csv_lock
def backup_incompatible_csv(path):
    """Rotate an existing CSV if its header does not match the current schema."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return

    expected = WEATHER_HEADER if path == LOCAL_WEATHER_CSV else SYSTEM_HEADER

    try:
        with open(path, "r", newline="") as handle:
            actual = next(csv.reader(handle), [])
    except Exception as exc:
        log(f"Could not inspect CSV header for {path}: {exc}", "WARNING")
        return

    if actual == expected:
        return

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"{path}.pre_refactor_{stamp}"
    os.replace(path, backup)
    log(f"CSV schema changed; preserved old file as {backup}", "WARNING")


def ensure_paths():
    os.makedirs(IMAGE_DIR, exist_ok=True)

    # A capture interrupted by a restart leaves its hidden temporary JPEG(s)
    # behind; they are never complete images, so remove them.
    for stale in glob.glob(os.path.join(IMAGE_DIR, ".*.jpg")):
        try:
            os.remove(stale)
            log(f"Removed incomplete image from interrupted capture: {stale}", "WARNING")
        except Exception as exc:
            log(f"Could not remove incomplete image {stale}: {exc}", "WARNING")

    backup_incompatible_csv(LOCAL_WEATHER_CSV)
    backup_incompatible_csv(LOCAL_SYSTEM_CSV)

    if not os.path.exists(LOCAL_WEATHER_CSV):
        write_csv_header(LOCAL_WEATHER_CSV, WEATHER_HEADER)

    if not os.path.exists(LOCAL_SYSTEM_CSV):
        write_csv_header(LOCAL_SYSTEM_CSV, SYSTEM_HEADER)


@with_csv_lock
def write_csv_header(path, header):
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        handle.flush()
        os.fsync(handle.fileno())


@with_csv_lock
def append_csv_row(path, row):
    """Append and fsync so a sudden power loss is less likely to lose a sample."""
    try:
        with open(path, "a", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(row)
            handle.flush()
            os.fsync(handle.fileno())
        return True
    except Exception as exc:
        log(f"CSV write failed for {path}: {exc}", "ERROR")
        return False


@with_csv_lock
def compact_csv_to_recent_rows(path, header, retain_days, reason):
    """Atomically bound a local telemetry CSV by its newest data rows.

    Row-count retention is deliberate: it remains reliable if timestamps are
    malformed and avoids loading the CSV into RAM.  The configured logging
    cadence determines the maximum row count.  The header is always retained.
    """
    rows_per_day = max(1, math.ceil(86400.0 / DATA_LOG_INTERVAL_S))
    keep_data_rows = max(1, int(retain_days * rows_per_day))
    temp_path = f"{path}.retention.tmp"

    try:
        with open(path, "r", newline="") as source:
            reader = csv.reader(source)
            actual_header = next(reader, None)
            if actual_header != header:
                log(
                    f"Skipping retention for {path}: unexpected CSV header",
                    "ERROR",
                )
                return False
            recent_rows = deque(maxlen=keep_data_rows)
            total_data_rows = 0
            for row in reader:
                recent_rows.append(row)
                total_data_rows += 1

        # Avoid rewriting files that are already inside the requested bound.
        if total_data_rows <= keep_data_rows:
            return True

        with open(temp_path, "w", newline="") as destination:
            writer = csv.writer(destination)
            writer.writerow(header)
            writer.writerows(recent_rows)
            destination.flush()
            os.fsync(destination.fileno())

        os.replace(temp_path, path)
        log(
            f"Compacted {path} to {len(recent_rows)} newest rows "
            f"({retain_days:g} days; {reason})"
        )
        return True
    except Exception as exc:
        log(f"CSV retention failed for {path}: {exc}", "ERROR")
        return False
    finally:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass


def enforce_local_csv_retention(retain_days, reason):
    compact_csv_to_recent_rows(
        LOCAL_WEATHER_CSV, WEATHER_HEADER, retain_days, reason
    )
    compact_csv_to_recent_rows(
        LOCAL_SYSTEM_CSV, SYSTEM_HEADER, retain_days, reason
    )


@with_csv_lock
def compact_csv_to_max_rows(path, header, max_data_rows, reason):
    """Apply an absolute row ceiling without assuming a sampling interval."""
    temp_path = f"{path}.retention.tmp"
    try:
        with open(path, "r", newline="") as source:
            reader = csv.reader(source)
            if next(reader, None) != header:
                log(f"Skipping hard retention for {path}: unexpected header", "ERROR")
                return False
            rows = deque(maxlen=max_data_rows)
            total = 0
            for row in reader:
                rows.append(row)
                total += 1
        if total <= max_data_rows:
            return True
        with open(temp_path, "w", newline="") as destination:
            writer = csv.writer(destination)
            writer.writerow(header)
            writer.writerows(rows)
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temp_path, path)
        log(f"Compacted {path} to {len(rows)} rows ({reason})", "WARNING")
        return True
    except Exception as exc:
        log(f"Hard CSV retention failed for {path}: {exc}", "ERROR")
        return False
    finally:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass


def load_connectivity_state():
    default = {"offline_since_epoch": None, "last_success_epoch": None}
    try:
        with open(CONNECTIVITY_STATE_PATH, "r", encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, dict):
            return default
        return {
            "offline_since_epoch": value.get("offline_since_epoch"),
            "last_success_epoch": value.get("last_success_epoch"),
        }
    except FileNotFoundError:
        return default
    except Exception as exc:
        log(f"Could not load connectivity state: {exc}", "WARNING")
        return default


def save_connectivity_state(state):
    temp_path = f"{CONNECTIVITY_STATE_PATH}.tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as handle:
            json.dump(state, handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, CONNECTIVITY_STATE_PATH)
    except Exception as exc:
        log(f"Could not save connectivity state: {exc}", "ERROR")
    finally:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass


def select_offline_policy(state):
    offline_since = state.get("offline_since_epoch")
    outage_seconds = 0.0
    if isinstance(offline_since, (int, float)):
        outage_seconds = max(0.0, time.time() - float(offline_since))

    policy_index = 0
    for index, policy in enumerate(OFFLINE_POLICIES):
        if outage_seconds >= policy[0]:
            policy_index = index

    usage = shutil.disk_usage(BASE_DIR)
    free_percent = 100.0 * usage.free / usage.total if usage.total else 0.0
    # Storage pressure can advance the policy early, but never makes it less
    # conservative than the elapsed-outage policy.
    if free_percent < 10.0:
        policy_index = len(OFFLINE_POLICIES) - 1
    elif free_percent < 20.0:
        policy_index = min(len(OFFLINE_POLICIES) - 1, policy_index + 1)

    return OFFLINE_POLICIES[policy_index], outage_seconds, free_percent


def enforce_image_disk_reserve():
    """Delete oldest local backlog images only when the reserve is threatened."""
    usage = shutil.disk_usage(BASE_DIR)
    reserve_bytes = max(
        int(MIN_FREE_DISK_GB * 1024 ** 3),
        int(usage.total * MIN_FREE_DISK_PERCENT / 100.0),
    )
    if usage.free >= reserve_bytes:
        return 0

    removed = 0
    for path in sorted(glob.glob(os.path.join(IMAGE_DIR, "*.jpg"))):
        try:
            os.remove(path)
            removed += 1
        except Exception as exc:
            log(f"Could not remove oldest backlog image {path}: {exc}", "ERROR")
        if shutil.disk_usage(BASE_DIR).free >= reserve_bytes:
            break

    if removed:
        log(
            f"Disk reserve protected by removing {removed} oldest local "
            "backlog image(s)",
            "WARNING",
        )
    return removed


# -----------------------------------------------------------------------------
# Raspberry Pi system telemetry
# -----------------------------------------------------------------------------

_psutil_primed = False


def get_cpu_temp():
    try:
        with open("/sys/class/thermal/thermal_zone0/temp", "r") as handle:
            return int(handle.read().strip()) / 1000.0
    except Exception as exc:
        log(f"CPU-temperature read failed: {exc}", "WARNING")
        return math.nan


def get_cpu_usage():
    global _psutil_primed
    try:
        if not _psutil_primed:
            psutil.cpu_percent(interval=None)
            _psutil_primed = True
        return float(psutil.cpu_percent(interval=None))
    except Exception as exc:
        log(f"CPU-usage read failed: {exc}", "WARNING")
        return math.nan


def get_memory_usage():
    try:
        return float(psutil.virtual_memory().percent)
    except Exception as exc:
        log(f"Memory-usage read failed: {exc}", "WARNING")
        return math.nan


def get_disk_free_gb():
    try:
        return float(psutil.disk_usage(BASE_DIR).free / (1024 ** 3))
    except Exception as exc:
        log(f"Disk-space read failed: {exc}", "WARNING")
        return math.nan


def get_throttled_hex():
    """Return Raspberry Pi firmware throttle flags, e.g. '0x50005'."""
    try:
        result = subprocess.run(
            ["vcgencmd", "get_throttled"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        output = result.stdout.strip()
        if "=" in output:
            return output.split("=", 1)[1].strip()
        return output or "unknown"
    except Exception as exc:
        log(f"Could not query vcgencmd get_throttled: {exc}", "WARNING")
        return "unknown"


# -----------------------------------------------------------------------------
# DHT22 switched power rail
# -----------------------------------------------------------------------------

def set_dht_power(enabled):
    """Drive the switched DHT supply on GPIO17 (physical pin 11).

    The two DHT VCC pins and their DATA pull-ups are powered from this GPIO.
    Driving it low therefore removes both VCC and pull-up power, giving the
    sensors the same cold reset that previously required unplugging the Pi.
    """
    if DHT_POWER_GPIO is None:
        return True

    pinctrl = shutil.which("pinctrl")
    if pinctrl is None:
        log("Cannot control DHT power: pinctrl command not found", "ERROR")
        return False

    drive = "dh" if enabled else "dl"
    try:
        result = subprocess.run(
            [pinctrl, "set", str(DHT_POWER_GPIO), "op", drive],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        state = "ON/HIGH" if enabled else "OFF/LOW"
        log(
            f"DHT switched power GPIO{DHT_POWER_GPIO} / physical pin "
            f"{DHT_POWER_PHYSICAL_PIN}: {state}"
        )
        return True
    except Exception as exc:
        log(
            f"Failed to set DHT power GPIO{DHT_POWER_GPIO} "
            f"{'HIGH' if enabled else 'LOW'}: {exc}",
            "ERROR",
        )
        return False


def ensure_dht_power_on(settle=True):
    """Power the DHT rail before creating any DHT driver objects."""
    if not DHT_GPIOS:
        return True
    if not set_dht_power(True):
        return False
    if settle:
        time.sleep(DHT_POWER_STARTUP_S)
    return True


def power_cycle_dht_readers(readers):
    """Cold-reset all configured DHTs and restart their sampler threads."""
    if not readers:
        return False

    log(
        "All DHT rolling buffers are fully invalid; beginning automatic "
        "cold power-cycle",
        "ERROR",
    )

    # No DHT object may be touching a data pin while VCC is being removed.
    for reader in readers:
        reader.close()

    if not set_dht_power(False):
        log("DHT cold reset aborted because power could not be switched off", "ERROR")
        for reader in readers:
            reader.restart_after_power_cycle()
        return False

    time.sleep(DHT_POWER_OFF_S)

    if not set_dht_power(True):
        log("DHT cold reset failed because power could not be restored", "ERROR")
        return False

    time.sleep(DHT_POWER_STARTUP_S)

    successes = []
    for reader in readers:
        successes.append(reader.restart_after_power_cycle())

    ok = all(successes)
    log(
        "DHT cold power-cycle complete; physical samplers restarted"
        if ok
        else "DHT cold power-cycle completed but one or more interfaces did not initialize",
        "INFO" if ok else "ERROR",
    )
    return ok


# -----------------------------------------------------------------------------
# DHT22 / AM2302
# -----------------------------------------------------------------------------

class DHT22Reader:
    """Continuously sampled DHT22 reader with interpolated logical output.

    Physical acquisition is independent of the weather-station scheduler.
    A background thread performs one DHT transaction every
    DHT_PHYSICAL_POLL_INTERVAL_S seconds and appends an entry to a rolling
    DHT_ROLLING_ATTEMPTS-element buffer:

        (sample_monotonic_time, temperature, humidity)

    Failed transactions append NaN values, so repeated failures naturally evict
    old successful measurements.

    read_once() is intentionally a *logical* read. It never accesses the DHT
    hardware. It evaluates the rolling buffer at

        time.monotonic() - DHT_INTERPOLATION_LOOKBACK_S

    using:
      * 0 valid samples: failure / NaN
      * 1 valid sample : return it directly
      * 2 valid samples: linear interpolation
      * 3+ valid samples: shape-preserving PCHIP interpolation

    Extrapolation is deliberately forbidden. If the requested target is outside
    the span of valid observations, the nearest valid observation is returned.
    """

    def __init__(self, pin, gpio, sensor_index):
        self.pin = pin
        self.gpio = int(gpio)
        self.sensor_index = int(sensor_index)
        self.label = f"DHT22[{self.sensor_index}] GPIO{self.gpio}"

        self.sensor = None
        self.consecutive_failures = 0
        self.last_attempt_monotonic = None
        self.last_good = (math.nan, math.nan)
        self.last_good_monotonic = None

        # One entry is retained for every physical attempt, including failures.
        # NaN therefore ages out old good data automatically.
        self._attempt_buffer = deque(maxlen=DHT_ROLLING_ATTEMPTS)
        self._buffer_lock = threading.Lock()

        # Only the background sampler touches the DHT object during normal
        # operation. The lock also makes shutdown/reinitialization race-safe.
        self._sensor_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._sampler_thread = None

        self.initialize()
        self._start_background_sampler()

    @property
    def available(self):
        with self._sensor_lock:
            return self.sensor is not None

    def _close_sensor_locked(self):
        if self.sensor is not None:
            try:
                self.sensor.exit()
            except Exception as exc:
                log(f"{self.label} cleanup warning: {exc}", "WARNING")
        self.sensor = None

    def _initialize_sensor_locked(self):
        self._close_sensor_locked()

        if adafruit_dht is None:
            log(f"DHT library unavailable: {DHT_IMPORT_ERROR}", "ERROR")
            return False

        try:
            # use_pulseio=False is the Linux/Raspberry Pi bit-bang path that
            # succeeds with this hardware.
            self.sensor = adafruit_dht.DHT22(
                self.pin,
                use_pulseio=False,
            )
            self.consecutive_failures = 0
            log(f"{self.label} hardware interface initialized")
            return True
        except Exception as exc:
            self.sensor = None
            log(f"{self.label} initialization failed: {exc}", "ERROR")
            return False

    def initialize(self):
        """Initialize/reinitialize only the hardware interface.

        The background sampling thread, if already running, is left intact.
        """
        with self._sensor_lock:
            return self._initialize_sensor_locked()

    def _start_background_sampler(self):
        if self._sampler_thread is not None and self._sampler_thread.is_alive():
            return

        self._stop_event.clear()
        self._sampler_thread = threading.Thread(
            target=self._sampler_loop,
            name=f"dht22-gpio{self.gpio}",
            daemon=True,
        )
        self._sampler_thread.start()

    def close(self):
        """Stop background acquisition and release the DHT interface."""
        self._stop_event.set()

        thread = self._sampler_thread
        if (
            thread is not None
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=max(3.0, DHT_PHYSICAL_POLL_INTERVAL_S + 1.0))

        self._sampler_thread = None

        with self._sensor_lock:
            self._close_sensor_locked()

    def _append_attempt(self, timestamp, temperature, humidity):
        with self._buffer_lock:
            self._attempt_buffer.append(
                (
                    float(timestamp),
                    float(temperature),
                    float(humidity),
                )
            )

    def _physical_sample(self):
        """Perform exactly one physical DHT transaction and buffer its result.

        A failed attempt always appends NaN/NaN, which is intentional: failures
        must consume buffer positions so stale measurements disappear naturally.
        """
        started = None
        temperature = math.nan
        humidity = math.nan
        success = False
        error = None

        # Serialise the actual timing-sensitive bit-banged transaction across
        # all DHT workers. The workers still poll independently; this only
        # prevents two GPIO transactions from overlapping by accident.
        with DHT_PHYSICAL_IO_LOCK:
            # Timestamp the actual transaction, not time spent waiting for the
            # shared DHT I/O lock.
            started = time.monotonic()
            with self._sensor_lock:
                if self.sensor is None:
                    if not self._initialize_sensor_locked():
                        error = "hardware interface unavailable"

                if self.sensor is not None:
                    try:
                        temperature = safe_float(self.sensor.temperature)
                        humidity = safe_float(self.sensor.humidity)

                        if not is_finite(temperature) or not is_finite(humidity):
                            raise RuntimeError("DHT22 returned non-finite data")
                        if not (-40.0 <= temperature <= 80.0):
                            raise RuntimeError(
                                f"DHT22 temperature out of range: {temperature}"
                            )
                        if not (0.0 <= humidity <= 100.0):
                            raise RuntimeError(
                                f"DHT22 humidity out of range: {humidity}"
                            )

                        success = True
                        self.consecutive_failures = 0
                        self.last_good = (temperature, humidity)
                        self.last_good_monotonic = time.monotonic()

                    except Exception as exc:
                        error = exc
                        self.consecutive_failures += 1

                        if (
                            self.consecutive_failures
                            >= DHT_REINITIALIZE_AFTER_FAILURES
                        ):
                            log(
                                f"{self.label} has failed "
                                f"{self.consecutive_failures} consecutive physical "
                                "reads; reinitializing interface",
                                "ERROR",
                            )
                            self._initialize_sensor_locked()
        finished = time.monotonic()
        if started is None:
            started = finished
        sample_time = 0.5 * (started + finished)
        self.last_attempt_monotonic = sample_time

        if success:
            self._append_attempt(sample_time, temperature, humidity)
        else:
            # Every failed physical attempt occupies a buffer position.
            self._append_attempt(sample_time, math.nan, math.nan)
            log(
                f"{self.label} physical read failed; buffered NaN "
                f"({self.consecutive_failures} consecutive): {error}",
                "WARNING",
            )

        return success

    def _sampler_loop(self):
        """Continuously sample this physical DHT at the safe sensor cadence."""
        # Slightly offset multiple DHT workers so their timing-sensitive
        # bit-banged transactions do not normally start at the same instant.
        initial_delay = max(
            0.0,
            (self.sensor_index - 1) * DHT_SENSOR_START_STAGGER_S,
        )
        if self._stop_event.wait(initial_delay):
            return

        next_attempt = time.monotonic()

        while not self._stop_event.is_set():
            now = time.monotonic()
            wait_s = next_attempt - now
            if wait_s > 0:
                if self._stop_event.wait(wait_s):
                    break

            attempt_started = time.monotonic()
            self._physical_sample()

            # Keep starts approximately DHT_PHYSICAL_POLL_INTERVAL_S apart.
            # If a transaction or OS scheduling delay overruns the interval,
            # schedule from "now" instead of trying to catch up with a burst.
            next_attempt += DHT_PHYSICAL_POLL_INTERVAL_S
            now = time.monotonic()
            if next_attempt <= now:
                next_attempt = now + DHT_PHYSICAL_POLL_INTERVAL_S

            # Defensive guard against an accidental zero/negative interval.
            if next_attempt <= attempt_started:
                next_attempt = (
                    attempt_started + DHT_PHYSICAL_POLL_INTERVAL_S
                )

    def rolling_health(self):
        """Return (attempt_count, valid_count) for power-reset decisions."""
        with self._buffer_lock:
            attempts = list(self._attempt_buffer)
        valid = sum(
            1
            for _timestamp, temperature, humidity in attempts
            if is_finite(temperature) and is_finite(humidity)
        )
        return len(attempts), valid

    def restart_after_power_cycle(self):
        """Clear stale state, recreate the DHT object, and restart sampling."""
        with self._buffer_lock:
            self._attempt_buffer.clear()

        self.consecutive_failures = 0
        self.last_attempt_monotonic = None
        self.last_good = (math.nan, math.nan)
        self.last_good_monotonic = None

        initialized = self.initialize()
        self._start_background_sampler()
        return initialized

    @staticmethod
    def _linear_interpolate(x0, y0, x1, y1, x):
        if x1 <= x0:
            return float(y0)
        fraction = (x - x0) / (x1 - x0)
        return float(y0 + fraction * (y1 - y0))

    @staticmethod
    def _pchip_slopes(xs, ys):
        """Return Fritsch-Carlson/PCHIP first derivatives.

        This small implementation avoids adding SciPy as a runtime dependency on
        the Raspberry Pi while retaining the no-overshoot behaviour wanted for
        slowly varying temperature/humidity data.
        """
        n = len(xs)
        if n < 2:
            return [0.0] * n

        h = [xs[i + 1] - xs[i] for i in range(n - 1)]
        delta = [
            (ys[i + 1] - ys[i]) / h[i]
            if h[i] > 0
            else 0.0
            for i in range(n - 1)
        ]

        if n == 2:
            return [delta[0], delta[0]]

        slopes = [0.0] * n

        # Interior derivatives: weighted harmonic mean when neighboring secant
        # slopes have the same sign; zero at a local extremum/flat transition.
        for i in range(1, n - 1):
            d_left = delta[i - 1]
            d_right = delta[i]

            if (
                d_left == 0.0
                or d_right == 0.0
                or (d_left > 0.0) != (d_right > 0.0)
            ):
                slopes[i] = 0.0
            else:
                w1 = 2.0 * h[i] + h[i - 1]
                w2 = h[i] + 2.0 * h[i - 1]
                slopes[i] = (w1 + w2) / (
                    (w1 / d_left) + (w2 / d_right)
                )

        def endpoint_slope(h0, h1, d0, d1):
            slope = ((2.0 * h0 + h1) * d0 - h0 * d1) / (h0 + h1)

            if slope == 0.0:
                return 0.0

            if (slope > 0.0) != (d0 > 0.0):
                return 0.0

            if (
                (d0 > 0.0) != (d1 > 0.0)
                and abs(slope) > 3.0 * abs(d0)
            ):
                return 3.0 * d0

            return slope

        slopes[0] = endpoint_slope(
            h[0],
            h[1],
            delta[0],
            delta[1],
        )
        slopes[-1] = endpoint_slope(
            h[-1],
            h[-2],
            delta[-1],
            delta[-2],
        )

        return slopes

    @classmethod
    def _pchip_interpolate(cls, xs, ys, target):
        """Evaluate shape-preserving cubic Hermite interpolation at target."""
        n = len(xs)
        if n == 1:
            return float(ys[0])
        if n == 2:
            return cls._linear_interpolate(
                xs[0], ys[0], xs[1], ys[1], target
            )

        # Exact sample time: preserve the actual observation exactly.
        for x, y in zip(xs, ys):
            if target == x:
                return float(y)

        interval = None
        for i in range(n - 1):
            if xs[i] <= target <= xs[i + 1]:
                interval = i
                break

        if interval is None:
            raise ValueError("PCHIP target lies outside sample range")

        slopes = cls._pchip_slopes(xs, ys)

        i = interval
        x0 = xs[i]
        x1 = xs[i + 1]
        h = x1 - x0
        if h <= 0:
            return float(ys[i])

        t = (target - x0) / h
        t2 = t * t
        t3 = t2 * t

        h00 = 2.0 * t3 - 3.0 * t2 + 1.0
        h10 = t3 - 2.0 * t2 + t
        h01 = -2.0 * t3 + 3.0 * t2
        h11 = t3 - t2

        return float(
            h00 * ys[i]
            + h10 * h * slopes[i]
            + h01 * ys[i + 1]
            + h11 * h * slopes[i + 1]
        )

    @classmethod
    def _estimate_channel(cls, points, target):
        """Estimate one channel at target without ever extrapolating.

        points is a sequence of (timestamp, value) pairs already filtered to
        finite values.
        """
        if not points:
            return math.nan, "none"

        points = sorted(points, key=lambda item: item[0])

        # Guard against duplicate timestamps, keeping the newest value for an
        # exact timestamp. Duplicates are unlikely with monotonic time but this
        # keeps interpolation numerically well-defined.
        deduplicated = []
        for timestamp, value in points:
            if deduplicated and timestamp == deduplicated[-1][0]:
                deduplicated[-1] = (timestamp, value)
            else:
                deduplicated.append((timestamp, value))
        points = deduplicated

        if len(points) == 1:
            return float(points[0][1]), "single-point"

        xs = [point[0] for point in points]
        ys = [point[1] for point in points]

        # Never extrapolate. The 2.1-s look-back normally ensures that target is
        # bracketed; if recent attempts failed, use the nearest actual reading.
        if target < xs[0] or target > xs[-1]:
            nearest = min(points, key=lambda item: abs(item[0] - target))
            return float(nearest[1]), "nearest-no-extrapolation"

        if len(points) == 2:
            return (
                cls._linear_interpolate(
                    xs[0], ys[0], xs[1], ys[1], target
                ),
                "linear",
            )

        return cls._pchip_interpolate(xs, ys, target), "pchip"

    def read_once(self):
        """Return one logical DHT reading from the rolling physical buffer.

        This preserves the old external interface:
            (temperature, humidity, ok)

        No DHT transaction occurs here.
        """
        request_time = time.monotonic()
        target_time = request_time - DHT_INTERPOLATION_LOOKBACK_S

        with self._buffer_lock:
            attempts = list(self._attempt_buffer)

        valid = [
            (timestamp, temperature, humidity)
            for timestamp, temperature, humidity in attempts
            if is_finite(temperature) and is_finite(humidity)
        ]

        if not valid:
            log(
                f"{self.label} logical read unavailable: "
                f"0/{len(attempts)} valid physical attempts in rolling buffer",
                "WARNING",
            )
            return math.nan, math.nan, False

        temperature_points = [
            (timestamp, temperature)
            for timestamp, temperature, _humidity in valid
        ]
        humidity_points = [
            (timestamp, humidity)
            for timestamp, _temperature, humidity in valid
        ]

        temperature, temperature_method = self._estimate_channel(
            temperature_points,
            target_time,
        )
        humidity, humidity_method = self._estimate_channel(
            humidity_points,
            target_time,
        )

        if not is_finite(temperature) or not is_finite(humidity):
            log(
                f"{self.label} logical interpolation returned non-finite data",
                "WARNING",
            )
            return math.nan, math.nan, False

        self.last_good = (temperature, humidity)
        self.last_good_monotonic = target_time

        method = (
            temperature_method
            if temperature_method == humidity_method
            else f"T={temperature_method},RH={humidity_method}"
        )

        log(
            f"{self.label} logical read: {temperature:.2f} C, "
            f"{humidity:.2f} % at t-"
            f"{DHT_INTERPOLATION_LOOKBACK_S:.1f}s; "
            f"{len(valid)}/{len(attempts)} valid attempts; method={method}"
        )

        return temperature, humidity, True


# -----------------------------------------------------------------------------
# BME280
# -----------------------------------------------------------------------------

class BME280Reader:
    """Fault-tolerant BME280 reader for I2C temperature/RH/pressure."""

    def __init__(self, address=BME280_I2C_ADDRESS):
        self.address = address
        self.enabled = address is not None
        self.i2c = None
        self.sensor = None
        self.consecutive_failures = 0
        self.last_attempt_monotonic = None
        self.last_good = (math.nan, math.nan, math.nan, math.nan)
        self.last_good_monotonic = None
        self.initialize()

    @property
    def available(self):
        return self.sensor is not None

    def initialize(self):
        self.close()

        if not self.enabled:
            return False

        if adafruit_bme280 is None:
            log(
                f"BME280 library unavailable: {BME280_IMPORT_ERROR}. "
                "Install adafruit-circuitpython-bme280.",
                "ERROR",
            )
            return False

        try:
            self.i2c = board.I2C()
            self.sensor = adafruit_bme280.Adafruit_BME280_I2C(
                self.i2c,
                address=self.address,
            )
            self.sensor.sea_level_pressure = BME280_SEA_LEVEL_PRESSURE_HPA
            self.consecutive_failures = 0
            log(f"BME280 initialized on I2C address 0x{self.address:02x}")
            return True
        except Exception as exc:
            self.sensor = None
            if self.i2c is not None:
                try:
                    self.i2c.deinit()
                except Exception:
                    pass
            self.i2c = None
            log(f"BME280 initialization failed: {exc}", "ERROR")
            return False

    def close(self):
        self.sensor = None
        if self.i2c is not None:
            try:
                self.i2c.deinit()
            except Exception as exc:
                log(f"BME280 I2C cleanup warning: {exc}", "WARNING")
        self.i2c = None

    @staticmethod
    def _validate_temperature(value):
        return value if is_finite(value) and -40.0 <= value <= 85.0 else math.nan

    @staticmethod
    def _validate_humidity(value):
        return value if is_finite(value) and 0.0 <= value <= 100.0 else math.nan

    @staticmethod
    def _validate_pressure(value):
        return value if is_finite(value) and 300.0 <= value <= 1100.0 else math.nan

    def read_once(self):
        """Perform one BME280 sample; caller controls the cadence."""
        if not self.enabled:
            return math.nan, math.nan, math.nan, math.nan, False

        if self.sensor is None and not self.initialize():
            return (*self.last_good, False)

        now = time.monotonic()
        if self.last_attempt_monotonic is not None:
            elapsed = now - self.last_attempt_monotonic
            if elapsed < BME280_POLL_INTERVAL_S:
                log(
                    f"BME280 read suppressed: only {elapsed:.2f}s since previous attempt",
                    "WARNING",
                )
                return math.nan, math.nan, math.nan, math.nan, False

        self.last_attempt_monotonic = now

        try:
            temperature = self._validate_temperature(safe_float(self.sensor.temperature))
            humidity = self._validate_humidity(safe_float(self.sensor.relative_humidity))
            pressure = self._validate_pressure(safe_float(self.sensor.pressure))
            altitude = safe_float(self.sensor.altitude)
            if not is_finite(altitude):
                altitude = math.nan

            if not all(is_finite(v) for v in (temperature, humidity, pressure)):
                raise RuntimeError(
                    f"BME280 returned invalid core data: "
                    f"T={temperature}, RH={humidity}, P={pressure}"
                )

            self.consecutive_failures = 0
            self.last_good = (temperature, humidity, pressure, altitude)
            self.last_good_monotonic = time.monotonic()
            log(
                f"BME280 read OK: {temperature:.2f} C, {humidity:.2f} %, "
                f"{pressure:.2f} hPa, altitude={altitude:.2f} m"
            )
            return temperature, humidity, pressure, altitude, True

        except Exception as exc:
            self.consecutive_failures += 1
            log(
                f"BME280 read failure ({self.consecutive_failures} consecutive): {exc}",
                "WARNING",
            )

        if self.consecutive_failures >= BME280_REINITIALIZE_AFTER_FAILURES:
            log(
                f"BME280 has failed {self.consecutive_failures} consecutive reads; "
                "reinitializing I2C interface",
                "ERROR",
            )
            self.initialize()

        return math.nan, math.nan, math.nan, math.nan, False

def combine_two_sensor_values(
    first_value,
    second_value,
    *,
    first_name,
    second_name,
    disagreement_threshold,
    unit,
):
    """Return the mean of available sensors, preserving single-sensor fallback."""
    values = [value for value in (first_value, second_value) if is_finite(value)]

    if len(values) == 2:
        difference = abs(values[0] - values[1])
        if difference > disagreement_threshold:
            log(
                f"{first_name} and {second_name} disagree by {difference:.2f} {unit}: "
                f"{first_name}={values[0]:.2f} {unit}, "
                f"{second_name}={values[1]:.2f} {unit}",
                "WARNING",
            )

    return mean_or_nan(values)


def combine_ambient_temperatures(dht_temperature_c, bme_temperature_c):
    return combine_two_sensor_values(
        dht_temperature_c,
        bme_temperature_c,
        first_name="DHT22",
        second_name="BME280",
        disagreement_threshold=TEMPERATURE_DISAGREEMENT_WARN_C,
        unit="C",
    )


def combine_ambient_humidities(dht_humidity_percent, bme_humidity_percent):
    return combine_two_sensor_values(
        dht_humidity_percent,
        bme_humidity_percent,
        first_name="DHT22",
        second_name="BME280",
        disagreement_threshold=HUMIDITY_DISAGREEMENT_WARN_PERCENT,
        unit="percentage points",
    )


def primary_with_fallback(primary_value, fallback_value):
    """Retain the current DHT22 default while allowing automatic BME fallback."""
    if is_finite(primary_value):
        return float(primary_value)
    if is_finite(fallback_value):
        return float(fallback_value)
    return math.nan


# -----------------------------------------------------------------------------
# Camera
# -----------------------------------------------------------------------------

class CameraManager:
    """Fault-tolerant day/twilight/night camera controller.

    Capture flow:
      1. Start in AE/AWB mode and meter the scene.
      2. Select DAY / TWILIGHT / NIGHT with hysteresis.
      3. DAY: fixed calibrated NoIR ColourGains/CCM/Saturation in libcamera.
      4. TWILIGHT: AE/AWB + low saturation; no fake colour reconstruction.
      5. NIGHT: restart with manual long exposure/high gain, monochrome,
         high-quality noise reduction where supported, and average 3-4 frames.
      6. Rotate every final image by 180 degrees.

    Every failure is contained so camera trouble cannot stop weather telemetry.
    """

    def __init__(self):
        self.camera = None
        self.last_lux = math.nan
        self.last_lux_monotonic = None
        self.scene_mode = None
        self.initialize()

    @property
    def available(self):
        return self.camera is not None

    def initialize(self):
        self.close()

        if Picamera2 is None:
            log(f"Picamera2 unavailable: {CAMERA_IMPORT_ERROR}", "ERROR")
            return False

        try:
            self.camera = Picamera2()
            # Apply the 180-degree orientation in libcamera itself. This rotates
            # all delivered frames before JPEG encoding, so orientation remains
            # correct even if Pillow/post-processing is unavailable.
            transform = (
                libcamera.Transform(hflip=True, vflip=True)
                if CAMERA_ROTATE_180
                else libcamera.Transform()
            )
            configuration = self.camera.create_still_configuration(transform=transform)
            self.camera.configure(configuration)
            log(
                "Camera initialized"
                + (" with 180-degree camera transform" if CAMERA_ROTATE_180 else "")
            )
            self._log_control_ranges()
            return True
        except Exception as exc:
            self.camera = None
            log(f"Camera initialization failed: {exc}", "ERROR")
            return False

    def close(self):
        if self.camera is not None:
            try:
                if getattr(self.camera, "started", False):
                    self.camera.stop()
            except Exception as exc:
                log(f"Camera stop during cleanup warning: {exc}", "WARNING")
            try:
                self.camera.close()
            except Exception as exc:
                log(f"Camera cleanup warning: {exc}", "WARNING")
        self.camera = None

    def _log_control_ranges(self):
        if self.camera is None:
            return
        for name in ("ExposureTime", "AnalogueGain", "FrameDurationLimits"):
            try:
                info = self.camera.camera_controls.get(name)
            except Exception:
                info = None
            if info is not None:
                log(f"Camera control {name} range/default: {info}")

    def lux_with_age(self):
        if not is_finite(self.last_lux) or self.last_lux_monotonic is None:
            return math.nan, math.nan
        return self.last_lux, max(0.0, time.monotonic() - self.last_lux_monotonic)

    @staticmethod
    def _metadata_stable(previous, current, threshold=CAMERA_STABLE_THRESHOLD):
        keys = ("ExposureTime", "AnalogueGain")
        compared = 0

        for key in keys:
            previous_value = safe_float(previous.get(key))
            current_value = safe_float(current.get(key))
            if not is_finite(previous_value) or not is_finite(current_value):
                continue
            if previous_value == 0:
                continue

            compared += 1
            relative_change = abs(current_value - previous_value) / abs(previous_value)
            if relative_change > threshold:
                return False

        return compared > 0

    @staticmethod
    def _hq_noise_reduction_value():
        """Return the libcamera HQ denoise enum across common API spellings."""
        if libcamera is None:
            return None
        try:
            enum = libcamera.controls.draft.NoiseReductionModeEnum
        except Exception:
            return None
        for attr in ("HighQuality", "NoiseReductionModeHighQuality"):
            if hasattr(enum, attr):
                return getattr(enum, attr)
        return None

    def _supported_controls(self, requested):
        """Drop controls not advertised by this camera/libcamera combination."""
        if self.camera is None:
            return {}
        try:
            available = self.camera.camera_controls
        except Exception:
            available = {}

        result = {}
        for name, value in requested.items():
            if value is None:
                continue
            if name in available:
                result[name] = value
            else:
                log(f"Camera control {name} not supported; skipping", "WARNING")
        return result

    def _set_controls(self, requested, context):
        controls = self._supported_controls(requested)
        if not controls:
            return True
        try:
            self.camera.set_controls(controls)
            log(f"Camera controls applied for {context}: {controls}")
            return True
        except Exception as exc:
            log(f"Camera controls failed for {context}: {exc}", "ERROR")
            return False

    def _auto_meter_controls(self):
        return {
            "AeEnable": True,
            "AwbEnable": True,
            "Saturation": CAMERA_METER_SATURATION,
            "Contrast": 1.0,
            "Sharpness": 1.1,
            "ExposureValue": 0.0,
            "NoiseReductionMode": self._hq_noise_reduction_value(),
        }

    def _day_controls(self):
        """Return the tested IMX477 NoIR daylight calibration."""
        return {
            "AeEnable": True,
            "AwbEnable": False,
            "ColourGains": (
                CAMERA_DAY_NOIR_RED_GAIN,
                CAMERA_DAY_NOIR_BLUE_GAIN,
            ),
            "ColourCorrectionMatrix": CAMERA_DAY_NOIR_CCM,
            "Saturation": CAMERA_DAY_NOIR_SATURATION,
            "Contrast": 1.0,
            "Sharpness": 1.1,
            "ExposureValue": 0.0,
            "NoiseReductionMode": self._hq_noise_reduction_value(),
        }

    def _twilight_controls(self):
        return {
            "AeEnable": True,
            "AwbEnable": True,
            "Saturation": CAMERA_TWILIGHT_SATURATION,
            "Contrast": 1.1,
            "Sharpness": 1.2,
            "ExposureValue": CAMERA_TWILIGHT_EXPOSURE_VALUE,
            "NoiseReductionMode": self._hq_noise_reduction_value(),
        }

    def _meter_scene(self):
        """Wait for AE/AGC to settle enough to classify scene brightness."""
        previous = None
        stable_count = 0
        latest = {}

        for frame in range(1, CAMERA_METER_MAX_FRAMES + 1):
            latest = self.camera.capture_metadata()

            if previous is not None and self._metadata_stable(previous, latest):
                stable_count += 1
            else:
                stable_count = 0

            lux = safe_float(latest.get("Lux"))
            exposure_us = safe_float(latest.get("ExposureTime"))
            gain = safe_float(latest.get("AnalogueGain"))
            log(
                f"Camera meter frame {frame}: lux={lux:.3f}, "
                f"exposure={exposure_us:.0f} us, gain={gain:.3f}, "
                f"stable={stable_count}/{CAMERA_METER_REQUIRED_STABLE}"
            )

            if stable_count >= CAMERA_METER_REQUIRED_STABLE:
                break
            previous = latest

        return latest

    def _classify_scene(self, metadata):
        lux = safe_float(metadata.get("Lux"))
        exposure_us = safe_float(metadata.get("ExposureTime"))
        gain = safe_float(metadata.get("AnalogueGain"))

        if is_finite(lux):
            previous_mode = self.scene_mode

            if previous_mode == "day":
                mode = "day" if lux >= CAMERA_DAY_EXIT_LUX else "twilight"
            elif previous_mode == "night":
                if lux >= CAMERA_DAY_ENTER_LUX:
                    mode = "day"
                elif lux >= CAMERA_NIGHT_EXIT_LUX:
                    mode = "twilight"
                else:
                    mode = "night"
            elif previous_mode == "twilight":
                if lux >= CAMERA_DAY_ENTER_LUX:
                    mode = "day"
                elif lux <= CAMERA_NIGHT_ENTER_LUX:
                    mode = "night"
                else:
                    mode = "twilight"
            else:
                if lux >= CAMERA_DAY_ENTER_LUX:
                    mode = "day"
                elif lux <= CAMERA_NIGHT_ENTER_LUX:
                    mode = "night"
                else:
                    mode = "twilight"
        else:
            # Fallback if this libcamera/tuning combination does not expose Lux.
            # ExposureTime*AnalogueGain is only a relative scene-brightness proxy,
            # but it is much safer than guessing from clock time.
            if is_finite(exposure_us) and is_finite(gain):
                exposure_product = exposure_us * gain
                if exposure_product < 50_000:
                    mode = "day"
                elif exposure_product < 400_000:
                    mode = "twilight"
                else:
                    mode = "night"
                log(
                    f"Camera Lux unavailable; classified from exposure product "
                    f"{exposure_product:.0f} us*gain",
                    "WARNING",
                )
            else:
                mode = self.scene_mode or "twilight"
                log(
                    "Camera has neither usable Lux nor exposure/gain metadata; "
                    f"retaining conservative mode={mode}",
                    "WARNING",
                )

        if mode != self.scene_mode:
            log(f"Camera scene mode transition: {self.scene_mode or 'unset'} -> {mode.upper()}")
        self.scene_mode = mode
        return mode

    def _clamp_scalar_control(self, name, value):
        try:
            minimum, maximum, _default = self.camera.camera_controls[name]
            if isinstance(minimum, (int, float)) and isinstance(maximum, (int, float)):
                return max(float(minimum), min(float(maximum), float(value)))
        except Exception:
            pass
        return float(value)

    def _clamp_frame_duration(self, value_us):
        value_us = int(value_us)
        try:
            minimum, maximum, _default = self.camera.camera_controls["FrameDurationLimits"]
            if isinstance(minimum, (tuple, list)) and isinstance(maximum, (tuple, list)):
                lower = max(float(x) for x in minimum)
                upper = min(float(x) for x in maximum)
                value_us = int(max(lower, min(upper, value_us)))
        except Exception:
            pass
        return value_us

    def _night_target(self, metadata):
        lux = safe_float(metadata.get("Lux"))
        metered_exposure = safe_float(metadata.get("ExposureTime"))
        metered_gain = safe_float(metadata.get("AnalogueGain"))

        base_exposure = 1_500_000
        base_gain = 8.0
        if is_finite(lux):
            for minimum_lux, exposure_us, gain in CAMERA_NIGHT_PROFILES:
                if lux >= minimum_lux:
                    base_exposure = exposure_us
                    base_gain = gain
                    break

        # Let the auto-metered values push us longer/higher, but never below the
        # profile minimum and never beyond the configured quality/noise limits.
        target_exposure = float(base_exposure)
        if is_finite(metered_exposure):
            target_exposure = max(target_exposure, metered_exposure * 2.0)
        target_exposure = min(target_exposure, CAMERA_NIGHT_MAX_EXPOSURE_US)
        target_exposure = self._clamp_scalar_control("ExposureTime", target_exposure)

        target_gain = float(base_gain)
        if is_finite(metered_gain):
            target_gain = max(target_gain, metered_gain)
        target_gain = min(target_gain, CAMERA_NIGHT_MAX_ANALOGUE_GAIN)
        target_gain = self._clamp_scalar_control("AnalogueGain", target_gain)

        exposure_us = int(round(target_exposure))
        gain = float(target_gain)
        frame_duration_us = self._clamp_frame_duration(
            exposure_us + CAMERA_NIGHT_FRAME_MARGIN_US
        )

        # Exposure cannot exceed the frame duration. Clamp once more if a camera
        # advertises a tighter frame-duration ceiling than its exposure ceiling.
        if frame_duration_us < exposure_us:
            exposure_us = frame_duration_us

        stack_count = (
            CAMERA_NIGHT_STACK_SHORT
            if exposure_us <= CAMERA_NIGHT_SHORT_EXPOSURE_US
            else CAMERA_NIGHT_STACK_LONG
        )

        return exposure_us, gain, frame_duration_us, stack_count

    def _night_controls(self, exposure_us, gain, frame_duration_us):
        return {
            "AeEnable": False,
            "AwbEnable": False,
            "ColourGains": (1.0, 1.0),
            "Saturation": 0.0,
            "Contrast": 1.15,
            "Sharpness": 1.2,
            "ExposureTime": int(exposure_us),
            "AnalogueGain": float(gain),
            "FrameDurationLimits": (int(frame_duration_us), int(frame_duration_us)),
            "NoiseReductionMode": self._hq_noise_reduction_value(),
        }

    def _wait_for_manual_controls(self, target_exposure_us, target_gain):
        """Confirm that manual exposure/gain actually reached completed frames."""
        latest = {}
        for frame in range(1, CAMERA_NIGHT_MANUAL_SETTLE_FRAMES + 1):
            latest = self.camera.capture_metadata()
            exposure = safe_float(latest.get("ExposureTime"))
            gain = safe_float(latest.get("AnalogueGain"))
            log(
                f"Night manual settle frame {frame}: exposure={exposure:.0f} us, "
                f"gain={gain:.3f}"
            )

            exposure_ok = (
                is_finite(exposure)
                and abs(exposure - target_exposure_us) <= max(5_000, target_exposure_us * 0.10)
            )
            gain_ok = (
                is_finite(gain)
                and abs(gain - target_gain) <= max(0.25, target_gain * 0.20)
            )
            if exposure_ok and gain_ok:
                return latest

        log(
            "Manual night exposure/gain did not confirm within settle-frame limit; "
            "capturing with the latest applied values",
            "WARNING",
        )
        return latest

    @staticmethod
    def _average_night_frames(frame_paths, output_path):
        """Average several monochrome JPEGs without a large NumPy allocation."""
        if Image is None:
            raise RuntimeError(f"Pillow unavailable: {PIL_IMPORT_ERROR}")
        if not frame_paths:
            raise RuntimeError("no night frames supplied for stacking")

        average = None
        count = 0
        for path in frame_paths:
            with Image.open(path) as opened:
                frame = opened.convert("L")
                if average is None:
                    average = frame.copy()
                    count = 1
                else:
                    count += 1
                    # Running arithmetic mean.  This keeps memory bounded to two
                    # image buffers and preserves moving animals as blur/ghosts
                    # rather than deleting them as a median stack might.
                    average = Image.blend(average, frame, 1.0 / count)

        if average is None:
            raise RuntimeError("night stack produced no image")

        average.convert("RGB").save(
            output_path,
            format="JPEG",
            quality=CAMERA_JPEG_QUALITY,
            optimize=True,
        )

    def _capture_night_stack(self, image_path, stack_count):
        # Pillow is used only for frame averaging. If it is unavailable, retain
        # a perfectly valid single manually-exposed frame rather than failing
        # the whole camera cycle. Rotation has already happened in libcamera.
        if Image is None:
            log(
                f"Pillow unavailable ({PIL_IMPORT_ERROR}); falling back to one "
                "unstacked night frame",
                "WARNING",
            )
            self.camera.capture_file(image_path)
            return False

        root, extension = os.path.splitext(image_path)
        temporary = []
        try:
            for index in range(stack_count):
                frame_path = f"{root}.nightstack_{index:02d}{extension}"
                self.camera.capture_file(frame_path)
                if not os.path.exists(frame_path) or os.path.getsize(frame_path) == 0:
                    raise RuntimeError(f"empty night frame: {frame_path}")
                temporary.append(frame_path)
                log(
                    f"Night frame {index + 1}/{stack_count} captured: "
                    f"{frame_path} ({os.path.getsize(frame_path)} bytes)"
                )

            self._average_night_frames(temporary, image_path)
            if not os.path.exists(image_path) or os.path.getsize(image_path) == 0:
                raise RuntimeError("averaged night JPEG is empty")
            log(f"Averaged {len(temporary)} night frames into {image_path}")
            return True

        except Exception as exc:
            # A valid last exposure is more valuable than no image. Preserve the
            # newest successfully captured frame as the final image if stacking
            # itself fails for any reason.
            if temporary:
                fallback = temporary[-1]
                try:
                    os.replace(fallback, image_path)
                    temporary.pop()
                    log(
                        f"Night stacking failed ({exc}); retained final single "
                        f"exposure as {image_path}",
                        "WARNING",
                    )
                    return False
                except Exception as fallback_exc:
                    log(
                        f"Night stacking failed ({exc}) and fallback frame could "
                        f"not be preserved: {fallback_exc}",
                        "ERROR",
                    )
            raise

        finally:
            for path in temporary:
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except Exception as exc:
                    log(f"Could not remove temporary night frame {path}: {exc}", "WARNING")


    def capture(self):
        """Capture one best-effort day/twilight/night image."""
        if self.camera is None and not self.initialize():
            return None

        camera = self.camera
        started = False
        image_path = None

        try:
            # Always meter afresh in full auto first.  This resets any manual
            # night controls left from the previous five-minute cycle.
            self._set_controls(self._auto_meter_controls(), "auto metering")
            camera.start()
            started = True

            metadata = self._meter_scene()
            lux = safe_float(metadata.get("Lux"))
            exposure_us = safe_float(metadata.get("ExposureTime"))
            gain = safe_float(metadata.get("AnalogueGain"))

            if is_finite(lux):
                self.last_lux = lux
                self.last_lux_monotonic = time.monotonic()
            else:
                log("Camera metadata did not contain a finite Lux value", "WARNING")

            mode = self._classify_scene(metadata)
            log(
                f"Camera selected {mode.upper()} profile from meter: "
                f"lux={lux:.3f}, exposure={exposure_us:.0f} us, gain={gain:.3f}"
            )

            stamp = time.strftime("%Y%m%d_%H%M%S")
            final_path = os.path.join(IMAGE_DIR, f"{stamp}.jpg")
            # Write under a hidden name and rename once complete: the
            # background upload only picks up *.jpg, so it never sees a
            # partially written image (night-stack frames are hidden too).
            image_path = os.path.join(IMAGE_DIR, f".{stamp}.jpg")

            if mode == "day":
                # Metering deliberately used normal AE/AWB. For the final DAY
                # JPEG, switch to the fixed NoIR calibration tested live with
                # rpicam-vid: ColourGains=(0.90, 1.30), Saturation=1.40, and
                # CCM=(1.35,-0.25,-0.10,-0.15,1.30,-0.15,-0.10,-0.25,1.35).
                self._set_controls(self._day_controls(), "day NoIR calibration")
                for frame in range(1, CAMERA_DAY_CONTROL_SETTLE_FRAMES + 1):
                    settled = camera.capture_metadata()
                    log(
                        f"Day NoIR settle frame {frame}/"
                        f"{CAMERA_DAY_CONTROL_SETTLE_FRAMES}: "
                        f"ColourGains={settled.get('ColourGains')}, "
                        f"exposure={safe_float(settled.get('ExposureTime')):.0f} us, "
                        f"gain={safe_float(settled.get('AnalogueGain')):.3f}"
                    )
                camera.capture_file(image_path)

            elif mode == "twilight":
                self._set_controls(self._twilight_controls(), "twilight")
                # Give changed ISP controls a couple of completed frames.
                camera.capture_metadata()
                camera.capture_metadata()
                camera.capture_file(image_path)

            else:  # NIGHT
                target_exposure, target_gain, frame_duration, stack_count = self._night_target(metadata)
                log(
                    f"Night target: exposure={target_exposure} us "
                    f"({target_exposure / 1e6:.2f} s), gain={target_gain:.2f}, "
                    f"frame_duration={frame_duration} us, stack={stack_count}"
                )

                # Picamera2 documents that controls set after configure but
                # before start apply to the first frame. Restarting here avoids
                # several-frame ambiguity for long manual exposures.
                camera.stop()
                started = False
                self._set_controls(
                    self._night_controls(target_exposure, target_gain, frame_duration),
                    "night manual",
                )
                camera.start()
                started = True
                self._wait_for_manual_controls(target_exposure, target_gain)
                self._capture_night_stack(image_path, stack_count)

            if not os.path.exists(image_path) or os.path.getsize(image_path) == 0:
                raise RuntimeError(f"camera produced an empty image: {image_path}")

            os.replace(image_path, final_path)
            log(
                f"Saved {mode.upper()} image: {final_path} "
                f"({os.path.getsize(final_path)} bytes)"
            )
            return final_path

        except Exception as exc:
            log(f"Camera capture failed: {exc}", "ERROR")
            if image_path is not None and os.path.exists(image_path):
                try:
                    os.remove(image_path)
                except Exception as cleanup_exc:
                    log(f"Could not remove incomplete image {image_path}: {cleanup_exc}", "WARNING")
            # Recreate the object on the next capture in case libcamera state is
            # wedged rather than permanently unavailable.
            self.close()
            return None

        finally:
            if started and self.camera is not None:
                try:
                    camera.stop()
                except Exception as exc:
                    log(f"Camera stop warning: {exc}", "WARNING")


# -----------------------------------------------------------------------------
# Local data collection
# -----------------------------------------------------------------------------

def write_data_block(dht_sample_map, bme_samples, camera_manager):
    """Aggregate current samples and append one weather/system row.

    dht_sample_map is keyed by BCM GPIO so every discovered DHT remains
    independently visible.  The historical DHT22_* columns are the ensemble of
    the individual DHT block medians.
    """
    dht_summaries = []
    dht_detail_values = []

    for gpio in DHT_GPIOS:
        samples = list(dht_sample_map.get(gpio, ()))
        temperature = median_or_nan([sample[0] for sample in samples])
        humidity = median_or_nan([sample[1] for sample in samples])
        dht_summaries.append(
            {
                "gpio": gpio,
                "temperature": temperature,
                "humidity": humidity,
                "count": len(samples),
            }
        )
        dht_detail_values.extend([gpio, temperature, humidity])

    dht_temperature = median_or_nan(
        [entry["temperature"] for entry in dht_summaries]
    )
    dht_humidity = median_or_nan(
        [entry["humidity"] for entry in dht_summaries]
    )

    bme_temperature = median_or_nan([sample[0] for sample in bme_samples])
    bme_humidity = median_or_nan([sample[1] for sample in bme_samples])
    bme_pressure = median_or_nan([sample[2] for sample in bme_samples])
    bme_altitude = median_or_nan([sample[3] for sample in bme_samples])

    combined_temperature = combine_ambient_temperatures(
        dht_temperature,
        bme_temperature,
    )
    combined_humidity = combine_ambient_humidities(
        dht_humidity,
        bme_humidity,
    )

    ambient_temperature = primary_with_fallback(dht_temperature, bme_temperature)
    ambient_humidity = primary_with_fallback(dht_humidity, bme_humidity)

    camera_lux, camera_lux_age = camera_manager.lux_with_age()
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")

    weather_ok = append_csv_row(
        LOCAL_WEATHER_CSV,
        [
            timestamp,
            ambient_temperature,
            ambient_humidity,
            combined_temperature,
            combined_humidity,
            dht_temperature,
            dht_humidity,
            *dht_detail_values,
            bme_temperature,
            bme_humidity,
            bme_pressure,
            bme_altitude,
            camera_lux,
            camera_lux_age,
        ],
    )

    cpu_temp = get_cpu_temp()
    cpu_usage = get_cpu_usage()
    memory_usage = get_memory_usage()
    disk_free_gb = get_disk_free_gb()
    throttled_hex = get_throttled_hex()

    system_ok = append_csv_row(
        LOCAL_SYSTEM_CSV,
        [
            timestamp,
            cpu_temp,
            cpu_usage,
            memory_usage,
            disk_free_gb,
            throttled_hex,
        ],
    )

    log("-----------------------------------------")
    log(f"Data block logged at {timestamp}")

    if dht_summaries:
        for index, entry in enumerate(dht_summaries, start=1):
            log(
                f"DHT22[{index}] GPIO{entry['gpio']}: "
                f"{entry['count']} valid samples; median "
                f"{entry['temperature']:.2f} C, {entry['humidity']:.2f} %"
            )
        log(
            f"DHT ensemble: {dht_temperature:.2f} C, "
            f"{dht_humidity:.2f} % RH"
        )
    else:
        log("No DHT22 sensors configured", "WARNING")

    if BME280_I2C_ADDRESS is None:
        log("BME280 disabled/not discovered")
    else:
        log(f"BME280 valid samples in block: {len(bme_samples)}")
        log(
            f"BME280 median: {bme_temperature:.2f} C, {bme_humidity:.2f} %, "
            f"{bme_pressure:.2f} hPa, altitude={bme_altitude:.2f} m"
        )

    log(
        f"Combined candidate: {combined_temperature:.2f} C, "
        f"{combined_humidity:.2f} % RH"
    )
    source = (
        "DHT ensemble"
        if is_finite(dht_temperature)
        else "BME280 fallback"
        if is_finite(bme_temperature)
        else "unavailable"
    )
    log(
        f"Current Ambient_* default: {ambient_temperature:.2f} C, "
        f"{ambient_humidity:.2f} % RH ({source})"
    )
    log(f"Camera lux: {camera_lux:.2f} (age {camera_lux_age:.1f} s)")
    log(f"CPU: {cpu_temp:.2f} C, {cpu_usage:.1f} %, RAM {memory_usage:.1f} %")
    log(f"Disk free: {disk_free_gb:.2f} GB; throttled={throttled_hex}")
    log("-----------------------------------------")

    return weather_ok and system_ok


# -----------------------------------------------------------------------------
# Transfers
# -----------------------------------------------------------------------------

def rsync_command(local_path, remote_spec):
    # --ignore-times: always transfer (still as a delta).  rsync's default
    # size + whole-second mtime check could otherwise skip a changed file.
    return [
        "rsync",
        "--ignore-times",
        f"--bwlimit={RSYNC_BANDWIDTH_LIMIT_KIB_S}",
        "--stats",
        "-e", f"ssh -o ConnectTimeout={RSYNC_CONNECT_TIMEOUT_S} -o BatchMode=yes",
        local_path,
        remote_spec,
    ]


def summarize_rsync_stats(output):
    """Reduce rsync --stats output to the byte counts worth logging."""
    wanted = ("Literal data", "Total bytes sent", "Total bytes received")
    parts = []
    for line in output.splitlines():
        key, separator, value = line.partition(":")
        if separator and key.strip() in wanted:
            parts.append(f"{key.strip().lower()} {value.strip()}")
    return "; ".join(parts)


def communicate_until(process, timeout_s, stop_event):
    """Like communicate(timeout=...), but also kill the process on shutdown."""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return process.communicate(timeout=1.0)
        except subprocess.TimeoutExpired:
            if stop_event.is_set() or time.monotonic() >= deadline:
                process.kill()
                process.communicate()
                raise


def rsync_with_retries(local_path, remote_spec, stop_event):
    for attempt in range(1, RSYNC_MAX_RETRIES + 1):
        if stop_event.is_set():
            return False
        log(f"Rsync attempt {attempt}/{RSYNC_MAX_RETRIES}: {local_path} -> {remote_spec}")
        started = time.monotonic()
        try:
            process = subprocess.Popen(
                rsync_command(local_path, remote_spec),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            stdout, stderr = communicate_until(
                process, RSYNC_PROCESS_TIMEOUT_S, stop_event
            )
            if process.returncode == 0:
                if stderr:
                    # OpenSSH writes verbose diagnostics to stderr even on success.
                    log(f"Rsync diagnostic output:\n{stderr.rstrip()}")
                log(
                    f"Copied successfully: {local_path} -> {remote_spec} "
                    f"({summarize_rsync_stats(stdout)}; "
                    f"{time.monotonic() - started:.1f} s)"
                )
                return True
            log(f"Rsync failed on attempt {attempt} with code {process.returncode}", "ERROR")
            if stdout:
                log(f"Rsync stdout:\n{stdout.rstrip()}", "ERROR")
            if stderr:
                log(f"Rsync stderr:\n{stderr.rstrip()}", "ERROR")

        except subprocess.TimeoutExpired:
            if stop_event.is_set():
                log(f"Rsync stopped for shutdown: {local_path}", "WARNING")
                return False
            log(
                f"Rsync timed out on attempt {attempt} after "
                f"{RSYNC_PROCESS_TIMEOUT_S} s",
                "ERROR",
            )
        except Exception as exc:
            log(f"Unexpected rsync failure on attempt {attempt}: {exc}", "ERROR")

        if attempt < RSYNC_MAX_RETRIES and stop_event.wait(RSYNC_RETRY_DELAY_S):
            return False

    return False


def upload_csv_snapshot(local_path, remote_path, stop_event):
    """Upload a copy of a telemetry CSV taken under CSV_LOCK.

    The live file keeps receiving rows while the copy is transferred; rows
    added after the snapshot go out with the next upload.
    """
    snapshot = os.path.join(UPLOAD_SNAPSHOT_DIR, os.path.basename(local_path))
    try:
        os.makedirs(UPLOAD_SNAPSHOT_DIR, exist_ok=True)
        with CSV_LOCK:
            shutil.copyfile(local_path, snapshot)
        return rsync_with_retries(
            snapshot, f"{SERVER_ADDRESS}:{remote_path}", stop_event
        )
    except Exception as exc:
        log(f"Could not prepare upload snapshot of {local_path}: {exc}", "ERROR")
        return False
    finally:
        try:
            os.remove(snapshot)
        except FileNotFoundError:
            pass
        except Exception as exc:
            log(f"Could not remove upload snapshot {snapshot}: {exc}", "WARNING")


def send_data(stop_event):
    """Transfer telemetry first, then drain a bounded slice of image backlog."""
    log("Beginning transfer phase")

    # Environmental/system telemetry is the primary product. Send it before
    # doing any camera work so camera delays or image backlog cannot block it.
    weather_ok = upload_csv_snapshot(
        LOCAL_WEATHER_CSV, SERVER_WEATHER_CSV, stop_event
    )
    system_ok = upload_csv_snapshot(
        LOCAL_SYSTEM_CSV, SERVER_SYSTEM_CSV, stop_event
    )

    if weather_ok and system_ok:
        log("Primary telemetry transfer complete: both CSV files uploaded")
    else:
        log(
            f"Primary telemetry transfer incomplete: weather={weather_ok}, "
            f"system={system_ok}",
            "ERROR",
        )

    # Only start backlog work when both primary telemetry streams reached the
    # server.  The per-cycle cap prevents months of images from blocking fresh
    # measurements after reconnection; repeated cycles eventually drain all of
    # them, oldest first.  Captures are written under a hidden name and renamed,
    # so every *.jpg listed here is a complete image.
    backlog = sorted(glob.glob(os.path.join(IMAGE_DIR, "*.jpg")))
    transferred = 0
    if weather_ok and system_ok:
        for image_path in backlog[:IMAGE_BACKLOG_UPLOADS_PER_CYCLE]:
            if not rsync_with_retries(
                image_path, f"{SERVER_ADDRESS}:{SERVER_IMAGE_DIR}", stop_event
            ):
                break
            try:
                os.remove(image_path)
                transferred += 1
                log(f"Removed transferred backlog image: {image_path}")
            except Exception as exc:
                log(
                    f"Image reached server but local cleanup failed "
                    f"for {image_path}: {exc}",
                    "WARNING",
                )

    remaining = max(0, len(backlog) - transferred)
    if remaining:
        log(
            f"Local image backlog: {remaining} file(s); transferred "
            f"{transferred} this cycle",
            "WARNING",
        )

    return weather_ok, system_ok


class UploadWorker:
    """Run send_data() in one background thread so sampling continues.

    At most one transfer runs at a time.  The result is handed back to the
    scheduler through a queue, so connectivity state and CSV compaction are
    still decided only in the main thread.
    """

    def __init__(self):
        self._thread = None
        self._results = queue.Queue()
        self._stop_event = threading.Event()

    def busy(self):
        return self._thread is not None and self._thread.is_alive()

    def start(self):
        # Also wait until the scheduler has taken the previous result.
        if self.busy() or not self._results.empty():
            return False
        self._thread = threading.Thread(
            target=self._run, name="upload", daemon=True
        )
        self._thread.start()
        return True

    def _run(self):
        started = time.monotonic()
        try:
            status = send_data(self._stop_event)
        except Exception as exc:
            # send_data is already defensive, but the worker itself is the
            # final containment boundary.
            log(f"Unhandled transfer-phase error: {exc}", "ERROR")
            status = (False, False)
        log(
            f"Transfer phase finished in {time.monotonic() - started:.1f} s: "
            f"weather={status[0]}, system={status[1]}"
        )
        self._results.put(status)

    def poll_result(self):
        """Return (weather_ok, system_ok) of a finished upload, else None."""
        try:
            return self._results.get_nowait()
        except queue.Empty:
            return None

    def stop(self):
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            log("Stopping background upload")
            thread.join(timeout=UPLOAD_STOP_TIMEOUT_S)
            if thread.is_alive():
                log("Background upload did not stop in time", "WARNING")


# -----------------------------------------------------------------------------
# Main scheduler
# -----------------------------------------------------------------------------

def main():
    ensure_paths()

    # GPIO17 / physical pin 11 is now the DHT supply.  Force it high before
    # constructing the Adafruit DHT objects; after a Pi reboot this GPIO would
    # otherwise begin as an input and both sensors would be unpowered.
    ensure_dht_power_on(settle=True)

    dht_readers = []
    for index, gpio in enumerate(DHT_GPIOS, start=1):
        pin = getattr(board, f"D{gpio}", None)
        if pin is None:
            log(f"Configured DHT22 GPIO{gpio} is unavailable in board module", "ERROR")
            continue
        dht_readers.append(DHT22Reader(pin, gpio, index))

    bme = BME280Reader(BME280_I2C_ADDRESS)
    camera = CameraManager()

    dht_samples = {
        reader.gpio: deque(maxlen=DHT_SAMPLE_BUFFER_SIZE)
        for reader in dht_readers
    }
    bme_samples = deque(maxlen=BME280_SAMPLE_BUFFER_SIZE)

    start = time.monotonic()
    next_dht_poll = start
    next_bme_poll = start
    next_data_log = start + DATA_LOG_INTERVAL_S
    next_upload = start + UPLOAD_INTERVAL_S
    next_image_capture = start + UPLOAD_INTERVAL_S
    next_retention_check = start + LOCAL_CSV_RETENTION_CHECK_INTERVAL_S
    connectivity_state = load_connectivity_state()
    last_policy_name = None

    uploader = UploadWorker()
    last_upload_status = (False, False)
    dht_power_failure_confirmations = 0
    last_dht_power_reset_monotonic = -math.inf

    log("Weather Station Initialized! Harvesting data...")
    dht_status = ", ".join(
        f"GPIO{reader.gpio}={'available' if reader.available else 'unavailable'}"
        for reader in dht_readers
    ) or "none configured"
    bme_status = (
        f"{'available' if bme.available else 'unavailable'} "
        f"at 0x{BME280_I2C_ADDRESS:02x}"
        if BME280_I2C_ADDRESS is not None
        else "disabled/not discovered"
    )
    log(
        f"DHT22 sensors: {dht_status}; "
        f"BME280={bme_status}; "
        f"camera={'available' if camera.available else 'unavailable'}"
    )
    log(f"Hardware configuration: {HARDWARE_CONFIG_PATH}")
    if DHT_GPIOS:
        log(
            f"DHT switched VCC: GPIO{DHT_POWER_GPIO} / physical pin "
            f"{DHT_POWER_PHYSICAL_PIN}; automatic cold-reset enabled"
        )
    log(
        f"Cadence: DHT physical={DHT_PHYSICAL_POLL_INTERVAL_S:.1f}s, "
        f"DHT logical={DHT_POLL_INTERVAL_S:.1f}s, "
        f"DHT look-back={DHT_INTERPOLATION_LOOKBACK_S:.1f}s, "
        f"BME280 poll={BME280_POLL_INTERVAL_S:.1f}s, "
        f"data log={DATA_LOG_INTERVAL_S:.1f}s, upload={UPLOAD_INTERVAL_S:.1f}s; "
        "adaptive offline store-and-forward enabled"
    )

    try:
        while True:
            now = time.monotonic()
            policy, outage_seconds, free_percent = select_offline_policy(
                connectivity_state
            )
            _minimum_outage, data_interval, image_interval, probe_interval, policy_name = policy
            if policy_name != last_policy_name:
                log(
                    f"Storage/network policy: {policy_name}; outage="
                    f"{outage_seconds / 3600.0:.1f} h; disk free="
                    f"{free_percent:.1f}%; telemetry={data_interval:.0f}s; "
                    f"images={image_interval:.0f}s; reconnect={probe_interval:.0f}s",
                    "WARNING" if connectivity_state.get("offline_since_epoch") else "INFO",
                )
                last_policy_name = policy_name

            if now >= next_dht_poll:
                for reader in dht_readers:
                    temperature, humidity, ok = reader.read_once()
                    if ok:
                        dht_samples[reader.gpio].append((temperature, humidity))

                # A true latch-up presents as a full rolling buffer of failed
                # physical attempts on every DHT.  Confirm this on successive
                # logical polls before touching power so ordinary checksum
                # glitches remain handled by the interpolation layer.
                if dht_readers:
                    health = [reader.rolling_health() for reader in dht_readers]
                    all_fully_dead = all(
                        attempts >= DHT_ROLLING_ATTEMPTS and valid == 0
                        for attempts, valid in health
                    )

                    if all_fully_dead:
                        dht_power_failure_confirmations += 1
                        health_text = ", ".join(
                            f"GPIO{reader.gpio}={valid}/{attempts}"
                            for reader, (attempts, valid) in zip(dht_readers, health)
                        )
                        log(
                            f"DHT hard-failure confirmation "
                            f"{dht_power_failure_confirmations}/"
                            f"{DHT_POWER_RESET_CONFIRMATIONS}: {health_text}",
                            "ERROR",
                        )
                    else:
                        dht_power_failure_confirmations = 0

                    since_reset = (
                        time.monotonic() - last_dht_power_reset_monotonic
                    )
                    if (
                        dht_power_failure_confirmations
                        >= DHT_POWER_RESET_CONFIRMATIONS
                        and since_reset >= DHT_POWER_RESET_COOLDOWN_S
                    ):
                        if power_cycle_dht_readers(dht_readers):
                            # Do not let pre-reset logical samples survive into
                            # the next 30-s aggregate block.
                            for samples in dht_samples.values():
                                samples.clear()
                        last_dht_power_reset_monotonic = time.monotonic()
                        dht_power_failure_confirmations = 0

                # Advance from 'now', rather than repeatedly catching up after a
                # long camera/SCP operation and accidentally bursting DHT reads.
                next_dht_poll = time.monotonic() + DHT_POLL_INTERVAL_S

            now = time.monotonic()
            if now >= next_bme_poll:
                if bme.enabled:
                    temperature, humidity, pressure, altitude, ok = bme.read_once()
                    if ok:
                        bme_samples.append(
                            (temperature, humidity, pressure, altitude)
                        )
                next_bme_poll = time.monotonic() + BME280_POLL_INTERVAL_S

            now = time.monotonic()
            if now >= next_data_log:
                write_data_block(dht_samples, list(bme_samples), camera)
                for samples in dht_samples.values():
                    samples.clear()
                bme_samples.clear()
                next_data_log = time.monotonic() + data_interval

            now = time.monotonic()
            if now >= next_image_capture:
                enforce_image_disk_reserve()
                image_path = camera.capture()
                if image_path is None:
                    log("Scheduled image capture produced no image", "WARNING")
                else:
                    log(f"Queued image for store-and-forward: {image_path}")
                enforce_image_disk_reserve()
                next_image_capture = time.monotonic() + image_interval

            # Uploads run in the background.  Start one when due; if the
            # previous one is still running, skip this cycle.  The finished
            # result is handled below exactly as the synchronous upload was,
            # including scheduling the next upload from its completion.
            now = time.monotonic()
            if now >= next_upload:
                if not uploader.start():
                    log("Previous upload still running; skipping this upload cycle", "WARNING")
                next_upload = time.monotonic() + UPLOAD_INTERVAL_S

            upload_result = uploader.poll_result()
            if upload_result is not None:
                last_upload_status = upload_result
                if all(last_upload_status):
                    was_offline = connectivity_state.get("offline_since_epoch")
                    connectivity_state = {
                        "offline_since_epoch": None,
                        "last_success_epoch": time.time(),
                    }
                    if was_offline is not None:
                        log("Connectivity restored; beginning bounded backlog drain")
                    next_upload_interval = UPLOAD_INTERVAL_S
                else:
                    if connectivity_state.get("offline_since_epoch") is None:
                        connectivity_state["offline_since_epoch"] = time.time()
                        log(
                            "Connectivity failure confirmed; entering adaptive "
                            "offline recording mode",
                            "WARNING",
                        )
                    next_upload_interval = probe_interval
                save_connectivity_state(connectivity_state)

                # A successful copy permits the shorter normal resend window.
                # Compact each stream independently: one successful upload must
                # never authorize trimming the other stream.
                if last_upload_status[0]:
                    compact_csv_to_recent_rows(
                        LOCAL_WEATHER_CSV,
                        WEATHER_HEADER,
                        LOCAL_CSV_RETAIN_AFTER_UPLOAD_DAYS,
                        "confirmed weather upload",
                    )
                if last_upload_status[1]:
                    compact_csv_to_recent_rows(
                        LOCAL_SYSTEM_CSV,
                        SYSTEM_HEADER,
                        LOCAL_CSV_RETAIN_AFTER_UPLOAD_DAYS,
                        "confirmed system upload",
                    )
                next_upload = time.monotonic() + next_upload_interval

            now = time.monotonic()
            if now >= next_retention_check:
                # At the long-term hourly cadence this ceiling represents more
                # than 28 years of measurements, while still making growth
                # mathematically finite if configuration changes unexpectedly.
                compact_csv_to_max_rows(
                    LOCAL_WEATHER_CSV,
                    WEATHER_HEADER,
                    LOCAL_CSV_ABSOLUTE_MAX_ROWS,
                    "absolute offline safety ceiling",
                )
                compact_csv_to_max_rows(
                    LOCAL_SYSTEM_CSV,
                    SYSTEM_HEADER,
                    LOCAL_CSV_ABSOLUTE_MAX_ROWS,
                    "absolute offline safety ceiling",
                )
                enforce_image_disk_reserve()
                next_retention_check = (
                    time.monotonic() + LOCAL_CSV_RETENTION_CHECK_INTERVAL_S
                )

            # Keep the control loop responsive without busy-spinning.  All real
            # sensor cadence is governed by monotonic deadlines above.
            time.sleep(0.2)

    except KeyboardInterrupt:
        log("Interrupted by user; shutting down")
    except Exception as exc:
        # If something genuinely unforeseen reaches this boundary, make it
        # visible and allow systemd to restart the service rather than silently
        # spinning in a corrupt state.
        log(f"Fatal scheduler error: {exc}", "ERROR")
        raise
    finally:
        uploader.stop()
        for reader in dht_readers:
            reader.close()
        bme.close()
        camera.close()
        log("Weather station shutdown complete")


def camera_test_once():
    """Capture one local image using the full day/twilight/night pipeline."""
    ensure_paths()
    camera = CameraManager()
    try:
        path = camera.capture()
        if path is None:
            log("Camera test failed", "ERROR")
            return 1
        log(f"Camera test complete: {path}")
        print(path)
        return 0
    finally:
        camera.close()


if __name__ == "__main__":
    if "--camera-test" in sys.argv:
        raise SystemExit(camera_test_once())
    main()
