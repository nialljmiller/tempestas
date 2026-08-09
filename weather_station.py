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
from datetime import datetime

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
except Exception as exc:
    Picamera2 = None
    CAMERA_IMPORT_ERROR = exc
else:
    CAMERA_IMPORT_ERROR = None


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

LOW_LIGHT_LUX = 100.0
CAMERA_STABLE_THRESHOLD = 0.02
CAMERA_REQUIRED_STABLE_ITERATIONS = 3
CAMERA_MAX_STABILIZATION_ITERATIONS = 30
CAMERA_STABILIZATION_SLEEP_S = 0.5

MAX_IMAGE_FILES = 100
SCP_MAX_RETRIES = 3
SCP_RETRY_DELAY_S = 5.0
SCP_BANDWIDTH_LIMIT_KBPS = "500"
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
    def __init__(self):
        self.camera = None
        self.last_lux = math.nan
        self.last_lux_monotonic = None
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
            self.camera.configure(self.camera.create_still_configuration())
            log("Camera initialized")
            return True
        except Exception as exc:
            self.camera = None
            log(f"Camera initialization failed: {exc}", "ERROR")
            return False

    def close(self):
        if self.camera is not None:
            try:
                self.camera.close()
            except Exception as exc:
                log(f"Camera cleanup warning: {exc}", "WARNING")
        self.camera = None

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
            log(f"Camera {key}: {current_value} (relative change {relative_change:.4f})")
            if relative_change > threshold:
                return False

        return compared > 0

    def capture(self):
        """Capture one image.  Failure is contained and returns None."""
        if self.camera is None and not self.initialize():
            return None

        camera = self.camera
        started = False

        try:
            camera.start()
            started = True
            camera.set_controls({
                "AeEnable": True,
                "AwbEnable": True,
                "Saturation": 1.0,
                "Contrast": 1.0,
                "Sharpness": 1.1,
            })

            time.sleep(0.5)
            metadata = camera.capture_metadata()
            lux = safe_float(metadata.get("Lux"))

            if is_finite(lux):
                self.last_lux = lux
                self.last_lux_monotonic = time.monotonic()
                log(f"Camera reported {lux:.2f} lux")
            else:
                log("Camera metadata did not contain a finite Lux value", "WARNING")

            low_light = is_finite(lux) and lux < LOW_LIGHT_LUX
            if low_light:
                log("Low light detected; applying low-light camera controls")
                camera.set_controls({
                    "AnalogueGain": 9.0,
                    "Saturation": 0.0,
                    "Contrast": 1.2,
                    "Sharpness": 1.5,
                })
                time.sleep(0.5)

            # Stabilization is camera-only.  It must never trigger DHT reads.
            if not low_light:
                previous = None
                stable_count = 0

                for iteration in range(1, CAMERA_MAX_STABILIZATION_ITERATIONS + 1):
                    camera.capture_array("main")
                    current = camera.capture_metadata()

                    if previous is not None:
                        if self._metadata_stable(previous, current):
                            stable_count += 1
                            log(
                                f"Camera stability check {stable_count}/"
                                f"{CAMERA_REQUIRED_STABLE_ITERATIONS}"
                            )
                        else:
                            stable_count = 0

                    previous = current

                    if stable_count >= CAMERA_REQUIRED_STABLE_ITERATIONS:
                        log("Camera settings stabilized")
                        break

                    time.sleep(CAMERA_STABILIZATION_SLEEP_S)
                else:
                    log(
                        "Camera did not meet stabilization threshold before limit; "
                        "capturing anyway",
                        "WARNING",
                    )

            stamp = time.strftime("%Y%m%d_%H%M%S")
            image_path = os.path.join(IMAGE_DIR, f"{stamp}.jpg")
            camera.capture_file(image_path)

            if not os.path.exists(image_path) or os.path.getsize(image_path) == 0:
                raise RuntimeError(f"camera produced an empty image: {image_path}")

            log(f"Saved image: {image_path} ({os.path.getsize(image_path)} bytes)")
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
                    self.camera.stop()
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
    timestamp = datetime.now().isoformat(timespec="seconds")

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
                    "-l", SCP_BANDWIDTH_LIMIT_KBPS,
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
    """Capture a picture and transfer pending images + both CSV files.

    Returns a pair (weather_csv_uploaded, system_csv_uploaded).  Image failures
    do not prevent environmental/system data from being transferred.
    """
    camera_manager.capture()
    log("Beginning transfer phase")

    image_files = sorted(glob.glob(os.path.join(IMAGE_DIR, "*.jpg")))
    if len(image_files) > MAX_IMAGE_FILES:
        # Do not delete the older backlog merely because it exceeds the normal
        # batch size.  Send the newest batch now and leave the rest for a later
        # recovery/manual decision.
        log(
            f"Image backlog contains {len(image_files)} files; sending newest "
            f"{MAX_IMAGE_FILES} this cycle",
            "WARNING",
        )
        image_files = image_files[-MAX_IMAGE_FILES:]

    if not image_files:
        log("No images pending transfer")

    for path in image_files:
        if scp_with_retries(path, f"{SERVER_ADDRESS}:{SERVER_IMAGE_DIR}"):
            try:
                os.remove(path)
                log(f"Removed transferred local image: {path}")
            except Exception as exc:
                log(f"Could not remove transferred image {path}: {exc}", "WARNING")
        else:
            log(f"Preserving image locally after failed transfer: {path}", "ERROR")

    weather_ok = scp_with_retries(
        LOCAL_WEATHER_CSV,
        f"{SERVER_ADDRESS}:{SERVER_WEATHER_CSV}",
    )
    system_ok = scp_with_retries(
        LOCAL_SYSTEM_CSV,
        f"{SERVER_ADDRESS}:{SERVER_SYSTEM_CSV}",
    )

    if weather_ok and system_ok:
        log("Transfer phase complete: both CSV files uploaded")
    else:
        log(
            f"Transfer phase incomplete: weather={weather_ok}, system={system_ok}",
            "ERROR",
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


if __name__ == "__main__":
    main()
