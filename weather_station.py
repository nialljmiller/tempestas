#!/usr/bin/env python3
"""Tempestas Raspberry Pi weather station.

Current hardware:
  * DHT22 / AM2302 on GPIO4: temperature + relative humidity
  * Raspberry Pi camera (Picamera2)

Future hardware:
  * Optional pressure sensor.  The pressure adapter deliberately has its own
    temperature channel so both the DHT22 and pressure-sensor temperatures are
    retained, plus a combined ambient-temperature estimate.

Design goals:
  * one failed sensor must not terminate the station
  * never poll the DHT22 faster than it should be polled
  * camera/network work must not trigger extra environmental-sensor reads
  * preserve useful diagnostic output under systemd
  * never silently invent replacement sensor values
  * never delete local CSV data unless the corresponding upload succeeded
"""

import csv
import glob
import math
import os
import statistics
import subprocess
import sys
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

SERVER_ADDRESS = "nill@nillmill.ddns.net"
SERVER_BASE = "/media/bigdata/weather_station"
SERVER_IMAGE_DIR = os.path.join(SERVER_BASE, "images/")
SERVER_WEATHER_CSV = os.path.join(SERVER_BASE, "weather_data.csv")
SERVER_SYSTEM_CSV = os.path.join(SERVER_BASE, "system_usage.csv")

# DHT22 / AM2302: Adafruit specifies no more than 0.5 Hz (one read / 2 s).
# Five seconds is intentionally conservative and gives six nominal samples per
# 30-second logged block without hammering a timing-sensitive sensor.
DHT_PIN = board.D4
DHT_POLL_INTERVAL_S = 5.0
DHT_REINITIALIZE_AFTER_FAILURES = 12
DHT_SAMPLE_BUFFER_SIZE = 24

DATA_LOG_INTERVAL_S = 30.0
UPLOAD_INTERVAL_S = 300.0
# Robustness default: keep the local CSV history indefinitely.  Set this to
# a positive number only if you explicitly want confirmed-upload cleanup.
LOCAL_CSV_CLEAR_INTERVAL_S = None

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

# Day profile: retain normal colour capture and correct the failed IR-cut filter
# afterwards.  Twilight deliberately desaturates because near-IR increasingly
# dominates and true colour becomes unreliable.
CAMERA_DAY_SATURATION = 1.0
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

# The camera's IR-cut filter has failed, producing a strong magenta cast in
# daylight.  This 3x3 RGB correction matrix was tuned against a representative
# image from this specific camera.  It is used only in DAY mode.
#
# PIL's RGB conversion matrix is:
#   R' = aR + bG + cB + d
#   G' = eR + fG + gB + h
#   B' = iR + jG + kB + l
CAMERA_IR_CUT_COMPENSATION = True
CAMERA_IR_COLOUR_MATRIX = (
    0.55, 0.05, 0.00, 0.0,
    0.25, 1.15, 0.00, 0.0,
    0.00, 0.05, 0.70, 0.0,
)
CAMERA_ROTATE_180 = True
CAMERA_JPEG_QUALITY = 95

SCP_MAX_RETRIES = 3
SCP_RETRY_DELAY_S = 5.0
SCP_BANDWIDTH_LIMIT_KBIT_S = "500"
SCP_CONNECT_TIMEOUT_S = "10"
SCP_PROCESS_TIMEOUT_S = 180

# Warn if two independent ambient-temperature sensors disagree substantially.
TEMPERATURE_DISAGREEMENT_WARN_C = 2.0

WEATHER_HEADER = [
    "Timestamp",
    "Ambient_Temperature_C",
    "DHT22_Temperature_C",
    "DHT22_Humidity_percent",
    "PressureSensor_Temperature_C",
    "Pressure_hPa",
    "Pressure_Altitude_m",
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

    backup_incompatible_csv(LOCAL_WEATHER_CSV)
    backup_incompatible_csv(LOCAL_SYSTEM_CSV)

    if not os.path.exists(LOCAL_WEATHER_CSV):
        write_csv_header(LOCAL_WEATHER_CSV, WEATHER_HEADER)

    if not os.path.exists(LOCAL_SYSTEM_CSV):
        write_csv_header(LOCAL_SYSTEM_CSV, SYSTEM_HEADER)


def write_csv_header(path, header):
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        handle.flush()
        os.fsync(handle.fileno())


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
# DHT22 / AM2302
# -----------------------------------------------------------------------------

class DHT22Reader:
    """Fault-tolerant DHT22 reader with controlled polling and reinitialization."""

    def __init__(self, pin):
        self.pin = pin
        self.sensor = None
        self.consecutive_failures = 0
        self.last_attempt_monotonic = None
        self.last_good = (math.nan, math.nan)
        self.last_good_monotonic = None
        self.initialize()

    @property
    def available(self):
        return self.sensor is not None

    def initialize(self):
        self.close()

        if adafruit_dht is None:
            log(f"DHT library unavailable: {DHT_IMPORT_ERROR}", "ERROR")
            return False

        try:
            # use_pulseio=False is the reliable Linux/Raspberry Pi path and is
            # the same mode that succeeded in the direct hardware test.
            self.sensor = adafruit_dht.DHT22(self.pin, use_pulseio=False)
            self.consecutive_failures = 0
            log("DHT22 initialized on GPIO4")
            return True
        except Exception as exc:
            self.sensor = None
            log(f"DHT22 initialization failed: {exc}", "ERROR")
            return False

    def close(self):
        if self.sensor is not None:
            try:
                self.sensor.exit()
            except Exception as exc:
                log(f"DHT22 cleanup warning: {exc}", "WARNING")
        self.sensor = None

    def read_once(self):
        """Perform at most one DHT transaction; caller controls the cadence."""
        if self.sensor is None and not self.initialize():
            return math.nan, math.nan, False

        now = time.monotonic()
        if self.last_attempt_monotonic is not None:
            elapsed = now - self.last_attempt_monotonic
            if elapsed < DHT_POLL_INTERVAL_S:
                # This is a programming/scheduler guard.  Never defeat the
                # cadence merely because another code path asks for data.
                log(
                    f"DHT22 read suppressed: only {elapsed:.2f}s since previous attempt",
                    "WARNING",
                )
                return math.nan, math.nan, False

        self.last_attempt_monotonic = now

        try:
            temperature = safe_float(self.sensor.temperature)
            humidity = safe_float(self.sensor.humidity)

            if not is_finite(temperature) or not is_finite(humidity):
                raise RuntimeError("DHT22 returned non-finite data")
            if not (-40.0 <= temperature <= 80.0):
                raise RuntimeError(f"DHT22 temperature out of range: {temperature}")
            if not (0.0 <= humidity <= 100.0):
                raise RuntimeError(f"DHT22 humidity out of range: {humidity}")

            self.consecutive_failures = 0
            self.last_good = (temperature, humidity)
            self.last_good_monotonic = time.monotonic()
            log(f"DHT22 read OK: {temperature:.1f} C, {humidity:.1f} %")
            return temperature, humidity, True

        except RuntimeError as exc:
            # Checksum/full-buffer errors are expected transient failure modes
            # for this sensor.  They are logged, not allowed to kill the daemon.
            self.consecutive_failures += 1
            log(
                f"DHT22 transient read failure "
                f"({self.consecutive_failures} consecutive): {exc}",
                "WARNING",
            )
        except Exception as exc:
            self.consecutive_failures += 1
            log(
                f"DHT22 unexpected read failure "
                f"({self.consecutive_failures} consecutive): {exc}",
                "ERROR",
            )

        if self.consecutive_failures >= DHT_REINITIALIZE_AFTER_FAILURES:
            log(
                f"DHT22 has failed {self.consecutive_failures} consecutive reads; "
                "reinitializing interface",
                "ERROR",
            )
            self.initialize()

        return math.nan, math.nan, False


# -----------------------------------------------------------------------------
# Optional future pressure sensor
# -----------------------------------------------------------------------------

class PressureReading:
    def __init__(self, temperature_c=math.nan, pressure_hpa=math.nan, altitude_m=math.nan):
        self.temperature_c = safe_float(temperature_c)
        self.pressure_hpa = safe_float(pressure_hpa)
        self.altitude_m = safe_float(altitude_m)


class NullPressureSensor:
    """Explicit no-hardware adapter; keeps pressure support optional."""

    name = "none"
    available = False

    def read(self):
        return PressureReading()

    def close(self):
        return None


def initialize_pressure_sensor():
    """Return the configured pressure-sensor adapter.

    There is intentionally no pressure sensor configured today.

    When one is added, put its hardware-specific initialization behind an
    adapter here.  Its read() method must return PressureReading containing:
      * temperature_c -- kept separately from DHT22 temperature
      * pressure_hpa
      * altitude_m     -- optional; may remain NaN

    The rest of the station does not need to change.  If the future sensor has
    a temperature channel, both raw temperatures are stored and both feed the
    Ambient_Temperature_C estimate.
    """
    return NullPressureSensor()


def read_pressure_sensor(sensor):
    try:
        reading = sensor.read()
        if reading is None:
            raise RuntimeError("pressure sensor returned None")
        return reading
    except Exception as exc:
        log(f"Pressure-sensor read failed ({getattr(sensor, 'name', 'unknown')}): {exc}", "WARNING")
        return PressureReading()


def combine_ambient_temperatures(dht_temperature_c, pressure_temperature_c):
    values = [
        value
        for value in (dht_temperature_c, pressure_temperature_c)
        if is_finite(value)
    ]

    if len(values) == 2:
        difference = abs(values[0] - values[1])
        if difference > TEMPERATURE_DISAGREEMENT_WARN_C:
            log(
                f"Temperature sensors disagree by {difference:.2f} C: "
                f"DHT22={values[0]:.2f} C, pressure sensor={values[1]:.2f} C",
                "WARNING",
            )

    return mean_or_nan(values)


# -----------------------------------------------------------------------------
# Camera
# -----------------------------------------------------------------------------

class CameraManager:
    """Fault-tolerant day/twilight/night camera controller.

    Capture flow:
      1. Start in AE/AWB mode and meter the scene.
      2. Select DAY / TWILIGHT / NIGHT with hysteresis.
      3. DAY: normal colour + failed-IR-cut correction in post-processing.
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
            "Saturation": CAMERA_DAY_SATURATION,
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


    @staticmethod
    def _postprocess_image(image_path, mode):
        """Apply daylight IR colour compensation after camera-side rotation."""
        needs_colour = CAMERA_IR_CUT_COMPENSATION and mode == "day"

        if not needs_colour:
            return True

        if Image is None:
            log(
                f"Image post-processing unavailable (Pillow import failed: "
                f"{PIL_IMPORT_ERROR}); retaining unprocessed image",
                "ERROR",
            )
            return False

        temp_path = f"{image_path}.postprocess.tmp"
        try:
            with Image.open(image_path) as opened:
                image = opened.convert("RGB")

                if needs_colour:
                    image = image.convert("RGB", CAMERA_IR_COLOUR_MATRIX)

                image.save(
                    temp_path,
                    format="JPEG",
                    quality=CAMERA_JPEG_QUALITY,
                    optimize=True,
                )

            if not os.path.exists(temp_path) or os.path.getsize(temp_path) == 0:
                raise RuntimeError("post-processed JPEG is empty")

            os.replace(temp_path, image_path)
            actions = []
            if needs_colour:
                actions.append("IR-cut daylight colour compensation")
            if mode == "twilight":
                actions.append("twilight low-saturation profile")
            if mode == "night":
                actions.append("night monochrome stack")
            log(f"Applied {' + '.join(actions)} to {image_path}")
            return True

        except Exception as exc:
            log(
                f"Image post-processing failed for {image_path}: {exc}; "
                "retaining original capture",
                "ERROR",
            )
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass
            return False

    def capture(self):
        """Capture one best-effort day/twilight/night image."""
        if self.camera is None and not self.initialize():
            return None

        camera = self.camera
        started = False

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
            image_path = os.path.join(IMAGE_DIR, f"{stamp}.jpg")

            if mode == "day":
                # Already in the appropriate auto profile; capture immediately.
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

            self._postprocess_image(image_path, mode=mode)

            log(
                f"Saved {mode.upper()} image: {image_path} "
                f"({os.path.getsize(image_path)} bytes)"
            )
            return image_path

        except Exception as exc:
            log(f"Camera capture failed: {exc}", "ERROR")
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

def write_data_block(dht_samples, pressure_sensor, camera_manager):
    """Aggregate current samples and append one weather/system row."""
    dht_temperatures = [sample[0] for sample in dht_samples]
    dht_humidities = [sample[1] for sample in dht_samples]

    dht_temperature = median_or_nan(dht_temperatures)
    dht_humidity = median_or_nan(dht_humidities)
    pressure = read_pressure_sensor(pressure_sensor)

    ambient_temperature = combine_ambient_temperatures(
        dht_temperature,
        pressure.temperature_c,
    )

    camera_lux, camera_lux_age = camera_manager.lux_with_age()
    # Store measurement timestamps as explicit UTC.
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")

    weather_ok = append_csv_row(
        LOCAL_WEATHER_CSV,
        [
            timestamp,
            ambient_temperature,
            dht_temperature,
            dht_humidity,
            pressure.temperature_c,
            pressure.pressure_hpa,
            pressure.altitude_m,
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
    log(f"DHT22 valid samples in block: {len(dht_samples)}")
    log(f"DHT22 median: {dht_temperature:.2f} C, {dht_humidity:.2f} %")
    if pressure_sensor.available:
        log(
            f"Pressure sensor ({pressure_sensor.name}): "
            f"{pressure.temperature_c:.2f} C, "
            f"{pressure.pressure_hpa:.2f} hPa, "
            f"{pressure.altitude_m:.2f} m"
        )
    else:
        log("Pressure sensor: not configured")
    log(f"Combined ambient temperature: {ambient_temperature:.2f} C")
    log(f"Camera lux: {camera_lux:.2f} (age {camera_lux_age:.1f} s)")
    log(f"CPU: {cpu_temp:.2f} C, {cpu_usage:.1f} %, RAM {memory_usage:.1f} %")
    log(f"Disk free: {disk_free_gb:.2f} GB; throttled={throttled_hex}")
    log("-----------------------------------------")

    return weather_ok and system_ok


# -----------------------------------------------------------------------------
# Transfers
# -----------------------------------------------------------------------------

def scp_with_retries(local_path, remote_spec):
    for attempt in range(1, SCP_MAX_RETRIES + 1):
        log(f"SCP attempt {attempt}/{SCP_MAX_RETRIES}: {local_path} -> {remote_spec}")
        try:
            result = subprocess.run(
                [
                    "scp",
                    "-v",  # retained intentionally: diagnostic evidence matters
                    "-l", SCP_BANDWIDTH_LIMIT_KBIT_S,
                    "-o", f"ConnectTimeout={SCP_CONNECT_TIMEOUT_S}",
                    local_path,
                    remote_spec,
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=SCP_PROCESS_TIMEOUT_S,
            )
            if result.stderr:
                # OpenSSH writes verbose diagnostics to stderr even on success.
                log(f"SCP diagnostic output:\n{result.stderr.rstrip()}")
            log(f"Copied successfully: {local_path} -> {remote_spec}")
            return True

        except subprocess.TimeoutExpired as exc:
            log(f"SCP timed out on attempt {attempt}: {exc}", "ERROR")
        except subprocess.CalledProcessError as exc:
            log(f"SCP failed on attempt {attempt} with code {exc.returncode}", "ERROR")
            if exc.stdout:
                log(f"SCP stdout:\n{exc.stdout.rstrip()}", "ERROR")
            if exc.stderr:
                log(f"SCP stderr:\n{exc.stderr.rstrip()}", "ERROR")
        except Exception as exc:
            log(f"Unexpected SCP failure on attempt {attempt}: {exc}", "ERROR")

        if attempt < SCP_MAX_RETRIES:
            time.sleep(SCP_RETRY_DELAY_S)

    return False


def send_data(camera_manager):
    """Transfer telemetry first, then capture/upload only the current image.

    Robustness rule: historical image backlog must never delay weather/system
    telemetry. Existing older images remain untouched in IMAGE_DIR for explicit
    manual recovery or archival. Returns (weather_csv_uploaded,
    system_csv_uploaded).
    """
    log("Beginning transfer phase")

    # Environmental/system telemetry is the primary product. Send it before
    # doing any camera work so camera delays or image backlog cannot block it.
    weather_ok = scp_with_retries(
        LOCAL_WEATHER_CSV,
        f"{SERVER_ADDRESS}:{SERVER_WEATHER_CSV}",
    )
    system_ok = scp_with_retries(
        LOCAL_SYSTEM_CSV,
        f"{SERVER_ADDRESS}:{SERVER_SYSTEM_CSV}",
    )

    if weather_ok and system_ok:
        log("Primary telemetry transfer complete: both CSV files uploaded")
    else:
        log(
            f"Primary telemetry transfer incomplete: weather={weather_ok}, "
            f"system={system_ok}",
            "ERROR",
        )

    # Capture one image for this cycle only. Older images are deliberately not
    # swept here; they must not turn a five-minute telemetry cycle into a long
    # backlog-draining job.
    image_path = camera_manager.capture()
    if image_path is None:
        log("No new image available for upload this cycle", "WARNING")
    else:
        if scp_with_retries(image_path, f"{SERVER_ADDRESS}:{SERVER_IMAGE_DIR}"):
            try:
                os.remove(image_path)
                log(f"Removed transferred current image: {image_path}")
            except Exception as exc:
                log(
                    f"Current image reached server but local cleanup failed "
                    f"for {image_path}: {exc}",
                    "WARNING",
                )
        else:
            log(
                f"Preserving current image locally after failed transfer: {image_path}",
                "ERROR",
            )

    # Make any pre-existing backlog explicit in the log without touching it.
    backlog = sorted(glob.glob(os.path.join(IMAGE_DIR, "*.jpg")))
    if backlog:
        log(
            f"Historical/local image backlog retained: {len(backlog)} file(s). "
            "Backlog does not block telemetry; transfer/archive it separately.",
            "WARNING",
        )

    return weather_ok, system_ok


def clear_uploaded_csvs(weather_uploaded, system_uploaded):
    """Clear only files known to have reached the server successfully."""
    if weather_uploaded:
        try:
            write_csv_header(LOCAL_WEATHER_CSV, WEATHER_HEADER)
            log("Cleared local weather CSV after confirmed upload")
        except Exception as exc:
            log(f"Could not clear local weather CSV: {exc}", "ERROR")
    else:
        log("Weather CSV not cleared because upload was not confirmed", "WARNING")

    if system_uploaded:
        try:
            write_csv_header(LOCAL_SYSTEM_CSV, SYSTEM_HEADER)
            log("Cleared local system CSV after confirmed upload")
        except Exception as exc:
            log(f"Could not clear local system CSV: {exc}", "ERROR")
    else:
        log("System CSV not cleared because upload was not confirmed", "WARNING")


# -----------------------------------------------------------------------------
# Main scheduler
# -----------------------------------------------------------------------------

def main():
    ensure_paths()

    dht = DHT22Reader(DHT_PIN)
    pressure_sensor = initialize_pressure_sensor()
    camera = CameraManager()

    dht_samples = deque(maxlen=DHT_SAMPLE_BUFFER_SIZE)

    start = time.monotonic()
    next_dht_poll = start
    next_data_log = start + DATA_LOG_INTERVAL_S
    next_upload = start + UPLOAD_INTERVAL_S
    next_clear = (
        start + LOCAL_CSV_CLEAR_INTERVAL_S
        if LOCAL_CSV_CLEAR_INTERVAL_S is not None
        else None
    )

    last_upload_status = (False, False)

    log("Weather Station Initialized! Harvesting data...")
    log(
        f"DHT22={'available' if dht.available else 'unavailable'}, "
        f"pressure={getattr(pressure_sensor, 'name', 'unknown')}, "
        f"camera={'available' if camera.available else 'unavailable'}"
    )
    log(
        f"Cadence: DHT poll={DHT_POLL_INTERVAL_S:.1f}s, "
        f"data log={DATA_LOG_INTERVAL_S:.1f}s, upload={UPLOAD_INTERVAL_S:.1f}s"
    )

    try:
        while True:
            now = time.monotonic()

            if now >= next_dht_poll:
                temperature, humidity, ok = dht.read_once()
                if ok:
                    dht_samples.append((temperature, humidity))

                # Advance from 'now', rather than repeatedly catching up after a
                # long camera/SCP operation and accidentally bursting DHT reads.
                next_dht_poll = time.monotonic() + DHT_POLL_INTERVAL_S

            now = time.monotonic()
            if now >= next_data_log:
                write_data_block(list(dht_samples), pressure_sensor, camera)
                dht_samples.clear()
                next_data_log = time.monotonic() + DATA_LOG_INTERVAL_S

            now = time.monotonic()
            if now >= next_upload:
                try:
                    last_upload_status = send_data(camera)
                except Exception as exc:
                    # send_data is already defensive, but the scheduler itself
                    # is the final containment boundary.
                    log(f"Unhandled transfer-phase error: {exc}", "ERROR")
                    last_upload_status = (False, False)
                next_upload = time.monotonic() + UPLOAD_INTERVAL_S

            now = time.monotonic()
            if next_clear is not None and now >= next_clear:
                clear_uploaded_csvs(*last_upload_status)
                # A success is consumed by the clear operation.  A later clear
                # must require a new confirmed upload, not a stale success flag.
                last_upload_status = (False, False)
                next_clear = time.monotonic() + LOCAL_CSV_CLEAR_INTERVAL_S

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
        dht.close()
        try:
            pressure_sensor.close()
        except Exception as exc:
            log(f"Pressure-sensor cleanup warning: {exc}", "WARNING")
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
