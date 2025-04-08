import glob
import numpy as np  # Use numpy for median calculation
import time
import board
import busio
import Adafruit_BMP.BMP085 as BMP085
import adafruit_dht
import adafruit_bh1750
import csv
import os
from datetime import datetime
import subprocess
import sys
import psutil  # For CPU and memory usage
from picamera2 import Picamera2



def get_cpu_temp():
    # Reads the CPU temperature
    try:
        with open('/sys/class/thermal/thermal_zone0/temp', 'r') as f:
            temp = int(f.read()) / 1000  # Convert millidegrees to Celsius
        return temp
    except FileNotFoundError:
        return "Unavailable"

def get_cpu_usage():
    # Gets the CPU usage as a percentage
    return psutil.cpu_percent(interval=1)

def get_memory_usage():
    # Gets the memory usage as a percentage
    memory = psutil.virtual_memory()
    return memory.percent

def makedata_time(sample_duration = 10, sample_interval = 1):
    # Set the collection duration and interval (in seconds)
    
    # Prepare lists to store readings
    bmp_temps = []
    pressures = []
    altitudes = []
    dht_temps = []
    humidities = []
    light_levels = []
    cpu_temps = []
    cpu_usages = []
    memory_usages = []

    # Collect data for sample_duration seconds
    end_time = time.time() + sample_duration
    while time.time() < end_time:
        try:
            # Gather individual sensor readings
            temperature_bmp = bmp_sensor.read_temperature()
            pressure = bmp_sensor.read_pressure() / 100  # hPa
            altitude = bmp_sensor.read_altitude()
            temperature_dht = dht_sensor.temperature
            humidity = dht_sensor.humidity
            light_level = light_sensor.lux

            cpu_temp = get_cpu_temp()
            cpu_usage = get_cpu_usage()
            memory_usage = get_memory_usage()

            # Append readings to respective lists
            bmp_temps.append(temperature_bmp)
            pressures.append(pressure)
            altitudes.append(altitude)
            dht_temps.append(temperature_dht)
            humidities.append(humidity)
            light_levels.append(light_level)
            cpu_temps.append(cpu_temp)
            cpu_usages.append(cpu_usage)
            memory_usages.append(memory_usage)
        except Exception as e:
            print(f"Error reading sensor: {e}")
            # A short delay before retrying
            time.sleep(sample_interval)
            continue

        time.sleep(sample_interval)

    # Compute median values for each sensor reading using numpy
    if bmp_temps:
        median_temperature_bmp = np.median(bmp_temps)
        median_pressure = np.median(pressures)
        median_altitude = np.median(altitudes)
        median_temperature_dht = np.median(dht_temps)
        median_humidity = np.median(humidities)
        median_light_level = np.median(light_levels)
        median_cpu_temp = np.median(cpu_temps)
        median_cpu_usage = np.median(cpu_usages)
        median_memory_usage = np.median(memory_usages)
    else:
        print("No samples collected!")
        return None

    # Use the current time as the timestamp for the median data set
    timestamp = datetime.now()
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")

    # Log median sensor data locally
    with open(local_csv, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([timestamp, median_temperature_bmp, median_pressure, median_altitude,
                         median_temperature_dht, median_humidity, median_light_level])

    # Log system data locally
    with open(system_csv_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([timestamp, median_cpu_temp, median_cpu_usage, median_memory_usage])

    print("\n\t-----------------------------------------")
    print(f"\tData logged at {timestamp}")
    print(f"\tMedian BMP Temperature: {median_temperature_bmp:.2f} °C, Pressure: {median_pressure:.2f} hPa, Altitude: {median_altitude:.2f} m")
    print(f"\tMedian DHT Temperature: {median_temperature_dht:.2f} °C, Humidity: {median_humidity:.2f} %")
    print(f"\tMedian BH1750 Light: {median_light_level:.2f} lx")
    print(f"\tMedian CPU Temperature: {median_cpu_temp}°C")
    print(f"\tMedian CPU Usage: {median_cpu_usage}%")
    print(f"\tMedian Memory Usage: {median_memory_usage}%")
    print(f"\tSamples made: {len(humidities)}")
    print("\t-----------------------------------------\n")
    return 



# Define light_level as a global variable
light_level = None

def makedata():
    global light_level  # Declare it as global to modify its value inside the function
    # Gather data
    timestamp = datetime.now()
    # Create a filesystem-safe timestamp string for the image filename
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    temperature_bmp = bmp_sensor.read_temperature()
    pressure = bmp_sensor.read_pressure() / 100  # Convert to hPa
    altitude = bmp_sensor.read_altitude()
    temperature_dht = dht_sensor.temperature
    humidity = dht_sensor.humidity
    light_level = light_sensor.lux

    cpu_temp = get_cpu_temp()
    cpu_usage = get_cpu_usage()
    memory_usage = get_memory_usage()


    # Log data locally
    with open(local_csv, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([timestamp, temperature_bmp, pressure, altitude,
                         temperature_dht, humidity, light_level])

    # Append the readings to the CSV file
    with open(system_csv_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([timestamp, cpu_temp, cpu_usage, memory_usage])

    print("\n\t-----------------------------------------")
    print(f"\tData logged at {timestamp}")
    print(f"\tBMP Temperature: {temperature_bmp:.2f} °C, Pressure: {pressure:.2f} hPa, Altitude: {altitude:.2f} m")
    print(f"\tDHT Temperature: {temperature_dht:.2f} °C, Humidity: {humidity:.2f} %")
    print(f"\tBH1750 Light: {light_level:.2f} lx")
    print(f"\tCPU Temperature: {cpu_temp}°C")
    print(f"\tCPU Usage: {cpu_usage}%")
    print(f"\tMemory Usage: {memory_usage}%")
    print("\t-----------------------------------------\n")



def is_stable(prev_meta, curr_meta, threshold=0.05):
    """
    Compare selected metadata values between two frames.
    Returns True if all values change less than the threshold (relative difference).
    """
    # List the keys you want to check. Adjust these keys based on what your camera provides.
    keys_to_check = ["ExposureTime", "AnalogGain"]
    
    for key in keys_to_check:
        if key in prev_meta and key in curr_meta:
            # Avoid division by zero in case a value is zero
            if prev_meta[key] == 0:
                continue
            print(f"{key} : {curr_meta[key]}")
            relative_change = abs(curr_meta[key] - prev_meta[key]) / prev_meta[key]
            if relative_change > threshold:
                return False
    return True




def take_pic():
    # Initialize the camera
    picam2.start()

    # Apply fully automatic settings
    picam2.set_controls({
        "AeEnable": True,  # Enable Auto Exposure
        "AwbEnable": True,  # Enable Auto White Balance
        "Saturation": 1.0,  # add color
        "Contrast": 1.0,  # 
        "Sharpness": 1.1,  # tiny Enhance details        
    })

    makedata()
    time.sleep(0.5)  # Allow auto-settings to initialize

    # Get initial light level from camera metadata
    metadata = picam2.capture_metadata()
    light_level = metadata.get("Lux", 200)  # Default to bright if value missing

    if light_level < 100:  # Low light condition
        print("Low light detected. Switching to IR mode...")
        picam2.set_controls({
            "AnalogueGain": 9.0,  # you might need to bump this up further if necessary
            "Saturation": 0.0,
            "Contrast": 1.2,
            "Sharpness": 1.5,
        })
        makedata()

    time.sleep(0.5)  # Allow settings to apply

    # Stabilization loop
    prev_metadata = None
    stable_count = 0
    required_stable_iterations = 3
    max_iterations = 30
    iteration = 0

    if light_level < 100:
        iteration = max_iterations

    while iteration < max_iterations:
        _ = picam2.capture_array("main")  # Dummy capture to update settings
        curr_metadata = picam2.capture_metadata()

        if prev_metadata is not None:
            # Check if settings have stabilized
            if is_stable(prev_metadata, curr_metadata, threshold=0.02):  
                stable_count += 1
                print(f"Stability check passed {stable_count}/{required_stable_iterations}")
            else:
                print("Settings fluctuating slightly...")
                # Only reset if the fluctuation is major
                if abs(prev_metadata["ExposureTime"] - curr_metadata["ExposureTime"]) > 5000:
                    stable_count = 0

        prev_metadata = curr_metadata
        iteration += 1
        makedata()
        if stable_count >= required_stable_iterations:
            print("Camera settings have stabilized.")
            break

        time.sleep(0.5)  # Reduced from 1s to speed up stabilization

    if iteration == max_iterations:
        print("Max iterations reached; proceeding with capture regardless.")

    # Capture final image
    timestamp_str = time.strftime("%Y%m%d_%H%M%S")
    image_filename = os.path.join(IMAGE_DIR, f"{timestamp_str}.jpg")
    picam2.capture_file(image_filename)

    picam2.stop()



def send_data():

    take_pic()
    print("Transferring data to the server...")
    try:
        max_retries = 3
        bandwidth_limit = "500"  # Bandwidth limit in Kbps
        connection_timeout = "10"  # Connection timeout in seconds

        # First SCP command
        for attempt in range(1, max_retries + 1):
            try:
                # Transfer images
                image_files = glob.glob(f"{IMAGE_DIR}*.jpg")  # Get a list of all .jpg files in the directory
                image_files = sorted(glob.glob(f"{IMAGE_DIR}*.jpg"))

                if len(image_files) > 100:
                    image_files = image_files[-100:]

                if image_files:
                    for image_file in image_files:
                        try:
                            subprocess.run(
                                ["scp", image_file, f"{server_address}:{SERVER_IMAGE_DIR}"],
                                check=True
                            )
                            print(f"Successfully transferred {image_file} to the server.")
                            os.remove(image_file)  # Delete the image after successful transfer
                            makedata()

                        except subprocess.CalledProcessError as e:
                            print(f"Error transferring {image_file}: {e}")
                            makedata()
                    break
                else:
                    print("No images found for transfer.")

            except subprocess.CalledProcessError as e:
                print(f"Error during copy attempt {attempt}: {e}")
                if attempt < max_retries:
                    print("Retrying...")
                    time.sleep(5)  # Wait 5 seconds before retrying
                    makedata()
                else:
                    print(f"All {max_retries} attempts failed for {IMAGE_DIR}.")
                    raise

        # First SCP command
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Attempt {attempt} to copy {local_csv} to {server_address}:{server_csv_path}")
                subprocess.run(
                    [
                        "scp", "-v", "-l", bandwidth_limit,
                        "-o", f"ConnectTimeout={connection_timeout}",
                        local_csv, f"{server_address}:{server_csv_path}"
                    ],
                    check=True
                )
                print(f"File {local_csv} successfully copied on attempt {attempt}.")
                break
            except subprocess.CalledProcessError as e:
                print(f"Error during copy attempt {attempt}: {e}")
                if attempt < max_retries:
                    print("Retrying...")
                    time.sleep(5)  # Wait 5 seconds before retrying
                else:
                    print(f"All {max_retries} attempts failed for {local_csv}.")
                    raise

        # Second SCP command
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Attempt {attempt} to copy {system_csv_file} to {server_address}:{system_server_csv_path}")
                subprocess.run(
                    [
                        "scp", "-v", "-l", bandwidth_limit,
                        "-o", f"ConnectTimeout={connection_timeout}",
                        system_csv_file, f"{server_address}:{system_server_csv_path}"
                    ],
                    check=True
                )
                print(f"File {system_csv_file} successfully copied on attempt {attempt}.")
                break
            except subprocess.CalledProcessError as e:
                print(f"Error during copy attempt {attempt}: {e}")
                if attempt < max_retries:
                    print("Retrying...")
                    time.sleep(5)  # Wait 5 seconds before retrying
                else:
                    print(f"All {max_retries} attempts failed for {system_csv_file}.")
                    raise

        print("Data successfully transferred to the server.")




    except subprocess.CalledProcessError as e:
        print(f"Error transferring data to the server: {e}\n")    

def del_data():
    # Clear local CSV files after successful transfer to avoid resending duplicate data
    with open(local_csv, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Timestamp", "BMP_Temperature_C", "BMP_Pressure_hPa", "BMP_Altitude_m",
                         "DHT_Temperature_C", "DHT_Humidity_percent", "BH1750_Light_lx"])

    with open(system_csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Timestamp", "CPU Temperature (°C)", "CPU Usage (%)", "Memory Usage (%)"])
    print("Local data cleared to save space.\n")

def initialize_light_sensor(i2c):
    """Initialize BH1750 with address auto-detection"""
    possible_addresses = [0x23, 0x5C]
    
    # Scan I2C bus to detect actual devices
    available_addresses = i2c.scan()
    
    # Try known BH1750 addresses in order of preference
    for addr in possible_addresses:
        if addr in available_addresses:
            try:
                return adafruit_bh1750.BH1750(i2c, address=addr)
            except Exception as e:
                print(f"Failed to initialize BH1750 at address 0x{addr:02x}: {e}")
    
    # No valid sensor found
    raise ValueError("BH1750 light sensor not detected at any known address")

sys.stdout.reconfigure(line_buffering=True)

# Initialize sensors
bmp_sensor = BMP085.BMP085()
dht_sensor = adafruit_dht.DHT11(board.D4)
i2c = busio.I2C(board.SCL, board.SDA)
light_sensor = initialize_light_sensor(i2c)

# File paths
system_csv_file = "/home/njm/system_usage.csv"
system_server_csv_path = "/media/bigdata/weather_station/system_usage.csv"  # File path on server
local_csv = "/home/njm/weather_data.csv"  # File on Raspberry Pi
server_csv_path = "/media/bigdata/weather_station/weather_data.csv"  # File path on server
server_address = "nill@nillmill.ddns.net"  # Server address

picam2 = Picamera2()
picam2.configure(picam2.create_still_configuration())

# File paths
BASE_DIR = "/home/njm/"
IMAGE_DIR = os.path.join(BASE_DIR, "images/")
CSV_FILES = {
    "local": os.path.join(BASE_DIR, "weather_data.csv"),
    "system": os.path.join(BASE_DIR, "system_usage.csv"),
    "server_local": "/media/bigdata/weather_station/weather_data.csv",
    "server_system": "/media/bigdata/weather_station/system_usage.csv",
}
SERVER_ADDRESS = "nill@nillmill.ddns.net"
SERVER_IMAGE_DIR = "/media/bigdata/weather_station/images/"

GOIO_PIN = 17

# Ensure local CSV exists
if not os.path.exists(local_csv):
    with open(local_csv, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Timestamp", "BMP_Temperature_C", "BMP_Pressure_hPa", "BMP_Altitude_m",
                         "DHT_Temperature_C", "DHT_Humidity_percent", "BH1750_Light_lx"])

# Check if the file exists, and if not, write the header
if not os.path.exists(system_csv_file):
    with open(system_csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Timestamp", "CPU Temperature (°C)", "CPU Usage (%)", "Memory Usage (%)"])

print("Weather Station Initialized! Harvesting data...\n")
write_timer = time.time()
del_timer = time.time()





while True:
    try:

        makedata()
        time_to_write = time.time() - write_timer
        time_to_del = time.time() - del_timer


        if time_to_write > 300:  # Every minute
            makedata()            
            send_data()
            write_timer = time.time()
            if time_to_del > 600:
                del_data()
                del_timer = time.time()

        makedata_time(sample_duration = 5, sample_interval = 0.1)
        makedata_time(sample_duration = 5, sample_interval = 0.1)

    except RuntimeError as e:
        # Handle sensor read errors
        print(f"Sensor error: {e}")
        time.sleep(2)
    except Exception as e:
        print(f"Unexpected error: {e}")
        break
