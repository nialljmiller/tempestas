#!/usr/bin/env python3
import glob
import os
import sys
import csv
import time
import subprocess
from datetime import datetime

import numpy as np
import psutil

import board
import busio
import Adafruit_BMP.BMP085 as BMP085
import adafruit_bh1750
from picamera2 import Picamera2

# Optional DHT; if missing or failing, we treat it as unavailable
try:
    import adafruit_dht
    _HAVE_DHT = True
except Exception:
    _HAVE_DHT = False

# -----------------------------
# Config
# -----------------------------
BASE_DIR = "/home/njm"
IMAGE_DIR = os.path.join(BASE_DIR, "images")
LOCAL_WEATHER_CSV = os.path.join(BASE_DIR, "weather_data.csv")
LOCAL_SYSTEM_CSV = os.path.join(BASE_DIR, "system_usage.csv")

SERVER_ADDRESS = "nill@nillmill.ddns.net"
SERVER_BASE = "/media/bigdata/weather_station"
SERVER_IMAGE_DIR = os.path.join(SERVER_BASE, "images/")
SERVER_WEATHER_CSV = os.path.join(SERVER_BASE, "weather_data.csv")
SERVER_SYSTEM_CSV = os.path.join(SERVER_BASE, "system_usage.csv")

LOW_LIGHT_LUX = 100.0
MAX_IMAGE_FILES = 100
SCP_MAX_RETRIES = 3
SCP_BANDWIDTH_LIMIT_KBPS = "500"
SCP_CONNECT_TIMEOUT_S = "10"

SAMPLE_BLOCK_SECONDS = 300        # send data every 5 minutes
DELETE_BLOCK_SECONDS = 600        # clear local CSVs every 10 minutes
MEDIAN_SAMPLES_DURATION = 5       # seconds per median block
MEDIAN_SAMPLES_INTERVAL = 0.1     # seconds between samples

sys.stdout.reconfigure(line_buffering=True)

# -----------------------------
# Utilities
# -----------------------------
def ensure_paths():
    os.makedirs(IMAGE_DIR, exist_ok=True)
    # ensure CSV headers exist
    if not os.path.exists(LOCAL_WEATHER_CSV):
        with open(LOCAL_WEATHER_CSV, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Timestamp",
                "BMP_Temperature_C",
                "BMP_Pressure_hPa",
                "BMP_Altitude_m",
                "DHT_Temperature_C",
                "DHT_Humidity_percent",
                "BH1750_Light_lx",
            ])
    if not os.path.exists(LOCAL_SYSTEM_CSV):
        with open(LOCAL_SYSTEM_CSV, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Timestamp",
                "CPU_Temperature_C",
                "CPU_Usage_percent",
                "Memory_Usage_percent",
            ])

def safe_float(x, default=np.nan):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def get_cpu_temp():
    try:
        with open("/sys/class/thermal/thermal_zone0/temp", "r") as f:
            return float(int(f.read()) / 1000.0)
    except Exception:
        return np.nan

# prime psutil to avoid first-call 0.0 quirk without blocking
_psutil_primed = False
def get_cpu_usage():
    global _psutil_primed
    if not _psutil_primed:
        psutil.cpu_percent(interval=None)
        _psutil_primed = True
    return psutil.cpu_percent(interval=None)

def get_memory_usage():
    return float(psutil.virtual_memory().percent)

def scp_with_retries(local_path, remote_spec):
    for attempt in range(1, SCP_MAX_RETRIES + 1):
        try:
            subprocess.run(
                [
                    "scp", "-v",
                    "-l", SCP_BANDWIDTH_LIMIT_KBPS,
                    "-o", f"ConnectTimeout={SCP_CONNECT_TIMEOUT_S}",
                    local_path, remote_spec
                ],
                check=True
            )
            print(f"✓ Copied: {local_path} -> {remote_spec}")
            return True
        except subprocess.CalledProcessError as e:
            print(f"✗ SCP error (attempt {attempt}): {e}")
            if attempt < SCP_MAX_RETRIES:
                time.sleep(5)
    return False

def is_stable(prev_meta, curr_meta, threshold=0.05):
    """
    Returns True if relative change in each key is <= threshold.
    Picamera2 metadata keys: 'ExposureTime', 'AnalogueGain', etc.
    """
    keys_to_check = ["ExposureTime", "AnalogueGain"]
    for key in keys_to_check:
        if key in prev_meta and key in curr_meta:
            pv = safe_float(prev_meta.get(key))
            cv = safe_float(curr_meta.get(key))
            if np.isnan(pv) or pv == 0 or np.isnan(cv):
                continue
            print(f"{key} : {cv}")
            rel = abs(cv - pv) / pv
            if rel > threshold:
                return False
    return True

# -----------------------------
# Sensor init
# -----------------------------
bmp_sensor = BMP085.BMP085()
dht_sensor = None
if _HAVE_DHT:
    try:
        dht_sensor = adafruit_dht.DHT11(board.D4)
    except Exception:
        dht_sensor = None

i2c = busio.I2C(board.SCL, board.SDA)

def initialize_light_sensor(i2c_bus):
    """Initialize BH1750 with address auto-detection."""
    possible_addresses = [0x23, 0x5C]
    available = set(i2c_bus.scan())
    for addr in possible_addresses:
        if addr in available:
            try:
                return adafruit_bh1750.BH1750(i2c_bus, address=addr)
            except Exception as e:
                print(f"BH1750 init failed at 0x{addr:02x}: {e}")
    raise RuntimeError("BH1750 light sensor not detected")

try:
    light_sensor = initialize_light_sensor(i2c)
except Exception as e:
    print(f"WARNING: Light sensor unavailable: {e}")
    light_sensor = None

# Camera
picam2 = Picamera2()
picam2.configure(picam2.create_still_configuration())

# -----------------------------
# Data capture
# -----------------------------
# numeric global to avoid None surprises
light_level = 0.0

def read_dht():
    """Return (temp_C, humidity_pct) or (nan, nan) if unavailable."""
    if dht_sensor is None:
        return (np.nan, np.nan)
    try:
        t = safe_float(dht_sensor.temperature)
        h = safe_float(dht_sensor.humidity)
        return (t, h)
    except Exception:
        return (np.nan, np.nan)

def read_bh1750():
    """Return lux as float or nan."""
    if light_sensor is None:
        return np.nan
    try:
        return safe_float(light_sensor.lux)
    except Exception:
        return np.nan

def makedata():
    """Single-shot sample and append to CSVs."""
    global light_level
    timestamp = datetime.now()
    temperature_bmp = safe_float(bmp_sensor.read_temperature())
    pressure = safe_float(bmp_sensor.read_pressure()) / 100.0  # hPa
    altitude = safe_float(bmp_sensor.read_altitude())
    light_level = read_bh1750()

    temperature_dht, humidity = read_dht()
    cpu_temp = get_cpu_temp()
    cpu_usage = get_cpu_usage()
    memory_usage = get_memory_usage()

    with open(LOCAL_WEATHER_CSV, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            timestamp, temperature_bmp, pressure, altitude,
            temperature_dht, humidity, light_level
        ])

    with open(LOCAL_SYSTEM_CSV, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, cpu_temp, cpu_usage, memory_usage])

    print("\n\t-----------------------------------------")
    print(f"\tData logged at {timestamp}")
    print(f"\tBMP Temperature: {temperature_bmp:.2f} °C, Pressure: {pressure:.2f} hPa, Altitude: {altitude:.2f} m")
    print(f"\tDHT Temperature: {temperature_dht:.2f} °C, Humidity: {humidity:.2f} %")
    print(f"\tBH1750 Light: {light_level:.2f} lx")
    print(f"\tCPU Temperature: {cpu_temp:.2f}°C")
    print(f"\tCPU Usage: {cpu_usage:.1f}%")
    print(f"\tMemory Usage: {memory_usage:.1f}%")
    print("\t-----------------------------------------\n")

def makedata_time(sample_duration=10, sample_interval=1.0):
    """
    Collect samples for a duration, aggregate with nanmedian, and append to CSVs.
    """
    bmp_temps, pressures, altitudes = [], [], []
    dht_temps, humidities, light_levels = [], [], []
    cpu_temps, cpu_usages, memory_usages = [], [], []

    end_time = time.time() + sample_duration
    while time.time() < end_time:
        try:
            bmp_temps.append(safe_float(bmp_sensor.read_temperature()))
            pressures.append(safe_float(bmp_sensor.read_pressure()) / 100.0)
            altitudes.append(safe_float(bmp_sensor.read_altitude()))
            light_levels.append(read_bh1750())

            t_dht, h_dht = read_dht()
            dht_temps.append(t_dht)
            humidities.append(h_dht)

            cpu_temps.append(get_cpu_temp())
            cpu_usages.append(get_cpu_usage())
            memory_usages.append(get_memory_usage())
        except Exception as e:
            print(f"Sensor read error (median block): {e}")
        time.sleep(sample_interval)

    if not bmp_temps:
        print("No samples collected in median block")
        return

    median_temperature_bmp = np.nanmedian(bmp_temps)
    median_pressure = np.nanmedian(pressures)
    median_altitude = np.nanmedian(altitudes)
    median_temperature_dht = np.nanmedian(dht_temps)
    median_humidity = np.nanmedian(humidities)
    median_light_level = np.nanmedian(light_levels)
    median_cpu_temp = np.nanmedian(cpu_temps)
    median_cpu_usage = np.nanmedian(cpu_usages)
    median_memory_usage = np.nanmedian(memory_usages)

    timestamp = datetime.now()

    with open(LOCAL_WEATHER_CSV, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            timestamp, median_temperature_bmp, median_pressure, median_altitude,
            median_temperature_dht, median_humidity, median_light_level
        ])

    with open(LOCAL_SYSTEM_CSV, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, median_cpu_temp, median_cpu_usage, median_memory_usage])

    print("\n\t-----------------------------------------")
    print(f"\tData logged at {timestamp}")
    print(f"\tMedian BMP Temperature: {median_temperature_bmp:.2f} °C, Pressure: {median_pressure:.2f} hPa, Altitude: {median_altitude:.2f} m")
    print(f"\tMedian DHT Temperature: {median_temperature_dht:.2f} °C, Humidity: {median_humidity:.2f} %")
    print(f"\tMedian BH1750 Light: {median_light_level:.2f} lx")
    print(f"\tMedian CPU Temperature: {median_cpu_temp:.2f}°C")
    print(f"\tMedian CPU Usage: {median_cpu_usage:.1f}%")
    print(f"\tMedian Memory Usage: {median_memory_usage:.1f}%")
    print(f"\tSamples made: {len(cpu_temps)}")
    print("\t-----------------------------------------\n")

def take_pic():
    """
    Capture a still with basic stabilization. Low light -> IR-ish settings.
    """
    picam2.start()
    picam2.set_controls({
        "AeEnable": True,
        "AwbEnable": True,
        "Saturation": 1.0,
        "Contrast": 1.0,
        "Sharpness": 1.1,
    })

    makedata()
    time.sleep(0.5)

    meta = picam2.capture_metadata()
    lux = meta.get("Lux")
    lux = safe_float(lux)
    if np.isnan(lux):
        # fallback to BH1750; else assume bright
        lux = read_bh1750()
        if np.isnan(lux):
            lux = 200.0

    if lux < LOW_LIGHT_LUX:
        print("Low light detected. Switching to IR mode...")
        picam2.set_controls({
            "AnalogueGain": 9.0,  # correct spelling
            "Saturation": 0.0,
            "Contrast": 1.2,
            "Sharpness": 1.5,
        })
        makedata()

    time.sleep(0.5)

    prev_metadata = None
    stable_count = 0
    required_stable_iterations = 3
    max_iterations = 30
    iteration = 0

    # Original behavior: skip stabilization in low light
    if lux < LOW_LIGHT_LUX:
        iteration = max_iterations

    while iteration < max_iterations:
        _ = picam2.capture_array("main")  # trigger AE/AWB updates
        curr_metadata = picam2.capture_metadata()

        if prev_metadata is not None:
            if is_stable(prev_metadata, curr_metadata, threshold=0.02):
                stable_count += 1
                print(f"Stability check passed {stable_count}/{required_stable_iterations}")
            else:
                print("Settings fluctuating slightly...")
                pv = safe_float(prev_metadata.get("ExposureTime"))
                cv = safe_float(curr_metadata.get("ExposureTime"))
                if not np.isnan(pv) and not np.isnan(cv) and abs(cv - pv) > 5000:
                    stable_count = 0

        prev_metadata = curr_metadata
        iteration += 1
        makedata()
        if stable_count >= required_stable_iterations:
            print("Camera settings have stabilized.")
            break

        time.sleep(0.5)

    if iteration == max_iterations:
        print("Max iterations reached; proceeding with capture regardless.")

    ts = time.strftime("%Y%m%d_%H%M%S")
    image_path = os.path.join(IMAGE_DIR, f"{ts}.jpg")
    picam2.capture_file(image_path)
    picam2.stop()
    print(f"Saved image: {image_path}")

def send_data():
    """
    Capture a picture, then transfer images and CSVs to the server with retries.
    Delete images locally if successfully transferred.
    """
    take_pic()
    print("Transferring data to the server...")

    # Transfer images (up to last MAX_IMAGE_FILES)
    image_files = sorted(glob.glob(os.path.join(IMAGE_DIR, "*.jpg")))
    if len(image_files) > MAX_IMAGE_FILES:
        image_files = image_files[-MAX_IMAGE_FILES:]

    if image_files:
        for path in image_files:
            ok = scp_with_retries(path, f"{SERVER_ADDRESS}:{SERVER_IMAGE_DIR}")
            if ok:
                try:
                    os.remove(path)
                except Exception as e:
                    print(f"Could not remove {path}: {e}")
            makedata()
    else:
        print("No images found for transfer.")

    # Transfer weather CSV
    ok = scp_with_retries(LOCAL_WEATHER_CSV, f"{SERVER_ADDRESS}:{SERVER_WEATHER_CSV}")
    if not ok:
        print(f"ERROR: Failed to transfer {LOCAL_WEATHER_CSV}")

    # Transfer system CSV
    ok = scp_with_retries(LOCAL_SYSTEM_CSV, f"{SERVER_ADDRESS}:{SERVER_SYSTEM_CSV}")
    if not ok:
        print(f"ERROR: Failed to transfer {LOCAL_SYSTEM_CSV}")

    print("Transfer phase complete.")

def del_data():
    """Clear local CSVs (retain headers)."""
    with open(LOCAL_WEATHER_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Timestamp",
            "BMP_Temperature_C",
            "BMP_Pressure_hPa",
            "BMP_Altitude_m",
            "DHT_Temperature_C",
            "DHT_Humidity_percent",
            "BH1750_Light_lx",
        ])

    with open(LOCAL_SYSTEM_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Timestamp",
            "CPU_Temperature_C",
            "CPU_Usage_percent",
            "Memory_Usage_percent",
        ])
    print("Local data cleared to save space.\n")

# -----------------------------
# Main
# -----------------------------
def main():
    ensure_paths()
    print("Weather Station Initialized! Harvesting data...\n")

    write_timer = time.time()
    del_timer = time.time()

    while True:
        try:
            makedata()

            now = time.time()
            if (now - write_timer) >= SAMPLE_BLOCK_SECONDS:
                makedata()
                send_data()
                write_timer = now

            if (now - del_timer) >= DELETE_BLOCK_SECONDS:
                del_data()
                del_timer = now

            # two median blocks per loop, as in your original flow
            makedata_time(sample_duration=MEDIAN_SAMPLES_DURATION, sample_interval=MEDIAN_SAMPLES_INTERVAL)
            makedata_time(sample_duration=MEDIAN_SAMPLES_DURATION, sample_interval=MEDIAN_SAMPLES_INTERVAL)

        except RuntimeError as e:
            print(f"Sensor error: {e}")
            time.sleep(2)
        except KeyboardInterrupt:
            print("Interrupted. Exiting.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break

if __name__ == "__main__":
    main()
