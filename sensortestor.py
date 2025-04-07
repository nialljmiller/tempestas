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
sys.stdout.reconfigure(line_buffering=True)



def median(data):
    # Sort the data
    sorted_data = sorted(data)
    n = len(sorted_data)

    # Check if the length of the list is odd
    if n % 2 == 1:
        # Return the middle element
        return sorted_data[n // 2]
    else:
        # Return the average of the two middle elements
        mid1 = sorted_data[n // 2 - 1]
        mid2 = sorted_data[n // 2]
        return (mid1 + mid2) / 2
        
        
# Initialize sensors
bmp_sensor = BMP085.BMP085()
dht_sensor = adafruit_dht.DHT11(board.D4)
i2c = busio.I2C(board.SCL, board.SDA)
#light_sensor = adafruit_bh1750.BH1750(i2c)
light_sensor = adafruit_bh1750.BH1750(i2c, resolution = HIGH)
# File paths
local_csv = "/home/njm/weather_data.csv"  # File on Raspberry Pi
server_csv_path = "/media/bigdata/weather_station/weather_data.csv"  # File path on server
server_address = "nill@nillmill.ddns.net"  # Server address

# Ensure local CSV exists
if not os.path.exists(local_csv):
    with open(local_csv, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Timestamp", "BMP_Temperature_C", "BMP_Pressure_hPa", "BMP_Altitude_m",
                         "DHT_Temperature_C", "DHT_Humidity_percent", "BH1750_Light_lx"])

print("Weather Station Initialized! Harvesting data...\n")
write_timer = time.time()

while True:
    try:
    
        temperature_bmp_l = []
        pressure_l = []
        altitude_l = []
        temperature_dht_l = []
        humidity_l = []
        light_level_l = []
    
    
        for i in range(10):        
            # Gather data
            temperature_bmp_l = bmp_sensor.read_temperature()
            pressure_l = bmp_sensor.read_pressure() / 100  # Convert to hPa
            altitude_l = bmp_sensor.read_altitude()
            temperature_dht_l = dht_sensor.temperature
            humidity_l = dht_sensor.humidity
            light_level_l = light_sensor.lux

            # Log data locally
            with open(local_csv, "a", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([timestamp, temperature_bmp, pressure, altitude,
                                 temperature_dht, humidity, light_level])
            if i == 4:
                timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


        temperature_bmp = median(temperature_bmp_l)
        pressure = median(pressure_l)
        altitude = median(altitude_l)
        temperature_dht = median(temperature_dht_l)
        humidity = median(humidity_l)
        light_level = median(light_level_l)



        print(f"Data logged at {timestamp}")
        print(f"BMP Temperature: {temperature_bmp:.2f} °C, Pressure: {pressure:.2f} hPa, Altitude: {altitude:.2f} m")
        print(f"DHT Temperature: {temperature_dht:.2f} °C, Humidity: {humidity:.2f} %")
        print(f"BH1750 Light: {light_level:.2f} lx\n")
        
        # Transfer data to server every minute
        print(time_to_write)



        time.sleep(10)  # Wait 10 seconds before the next reading



        time_to_write = time.time() - write_timer
        if time_to_write > 60:  # Every minute
            write_timer = time.time()
            print("Transferring data to the server...")
            try:
                subprocess.run(
                    ["scp", local_csv, f"{server_address}:{server_csv_path}"],
                    check=True
                )
                print("Data successfully transferred to the server.")
                # Clear local CSV file after successful transfer
                with open(local_csv, "w", newline="") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(["Timestamp", "BMP_Temperature_C", "BMP_Pressure_hPa", "BMP_Altitude_m",
                                     "DHT_Temperature_C", "DHT_Humidity_percent", "BH1750_Light_lx"])
                print("Local data cleared to save space.\n")
            except subprocess.CalledProcessError as e:
                print(f"Error transferring data to the server: {e}\n")

    except RuntimeError as e:
        # Handle sensor read errors
        print(f"Sensor error: {e}")
        time.sleep(2)
    except Exception as e:
        print(f"Unexpected error: {e}")
        break

