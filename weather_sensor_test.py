#!/usr/bin/env python3

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Raspberry Pi 40-pin header
# value = (description, BCM GPIO or None)
# ---------------------------------------------------------------------------

HEADER = {
     1: ("3V3", None),
     2: ("5V", None),
     3: ("SDA1", 2),
     4: ("5V", None),
     5: ("SCL1", 3),
     6: ("GND", None),
     7: ("GPIO4", 4),
     8: ("TXD0", 14),
     9: ("GND", None),
    10: ("RXD0", 15),
    11: ("GPIO17", 17),
    12: ("GPIO18", 18),
    13: ("GPIO27", 27),
    14: ("GND", None),
    15: ("GPIO22", 22),
    16: ("GPIO23", 23),
    17: ("3V3", None),
    18: ("GPIO24", 24),
    19: ("MOSI", 10),
    20: ("GND", None),
    21: ("MISO", 9),
    22: ("GPIO25", 25),
    23: ("SCLK", 11),
    24: ("CE0", 8),
    25: ("GND", None),
    26: ("CE1", 7),
    27: ("ID_SD", 0),
    28: ("ID_SC", 1),
    29: ("GPIO5", 5),
    30: ("GND", None),
    31: ("GPIO6", 6),
    32: ("GPIO12", 12),
    33: ("GPIO13", 13),
    34: ("GND", None),
    35: ("GPIO19", 19),
    36: ("GPIO16", 16),
    37: ("GPIO26", 26),
    38: ("GPIO20", 20),
    39: ("GND", None),
    40: ("GPIO21", 21),
}

GPIO_TO_PHYS = {
    gpio: phys
    for phys, (_, gpio) in HEADER.items()
    if gpio is not None
}

HARDWARE_CONFIG_PATH = Path("/home/njm/weather_hardware.json")
WEATHER_SERVICE = "weather_station.service"


# GPIO0/1 are normally reserved for HAT EEPROM.
# GPIO2/3 are deliberately protected because the BME280 uses I2C there.
# GPIO17 / physical pin 11 is no longer a DHT data candidate: it is the
# software-switchable 3.3-V supply for both DHT sensors.
DHT_POWER_GPIO = 17
DHT_POWER_PHYSICAL_PIN = 11
DHT_POWER_OFF_S = 5.0
DHT_POWER_STARTUP_S = 3.0
PROTECTED_DHT_GPIOS = {0, 1, 2, 3, DHT_POWER_GPIO}

# Probe likely general-purpose pins first.
DHT_PRIORITY = [
    4, 9,
    27, 22,
    23, 24, 25,
    5, 6,
    12, 13, 16,
    18, 19, 20, 21,
    26,
    7, 8, 10, 11,
    14, 15,
]


def heading(text):
    print()
    print("=" * 78)
    print(text)
    print("=" * 78)


# ---------------------------------------------------------------------------
# GPIO state
# ---------------------------------------------------------------------------

def gpio_state(gpio):
    """Return the pinctrl/raspi-gpio description for a BCM GPIO."""

    pinctrl = shutil.which("pinctrl")
    if pinctrl:
        try:
            p = subprocess.run(
                [pinctrl, "get", str(gpio)],
                capture_output=True,
                text=True,
                timeout=3,
            )
            text = (p.stdout or p.stderr).strip()
            if text:
                return text
        except Exception:
            pass

    raspi_gpio = shutil.which("raspi-gpio")
    if raspi_gpio:
        try:
            p = subprocess.run(
                [raspi_gpio, "get", str(gpio)],
                capture_output=True,
                text=True,
                timeout=3,
            )
            text = (p.stdout or p.stderr).strip()
            if text:
                return text
        except Exception:
            pass

    return None


def gpio_is_input(state):
    if not state:
        return False

    # pinctrl example:
    # 4: ip    -- | hi // GPIO4 = input
    if re.search(r":\s+ip\b", state):
        return True

    # older raspi-gpio example
    if "func=INPUT" in state.upper():
        return True

    return False


def gpio_level(state):
    if not state:
        return "?"

    if re.search(r"\|\s+hi\b", state):
        return "HIGH"

    if re.search(r"\|\s+lo\b", state):
        return "LOW"

    m = re.search(r"level=(\d)", state)
    if m:
        return "HIGH" if m.group(1) == "1" else "LOW"

    return "?"


def set_gpio_output(gpio, high):
    """Set one BCM GPIO as an output using pinctrl (or raspi-gpio fallback)."""
    pinctrl = shutil.which("pinctrl")
    if pinctrl:
        drive = "dh" if high else "dl"
        try:
            p = subprocess.run(
                [pinctrl, "set", str(gpio), "op", drive],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if p.returncode == 0:
                return True, None
            return False, (p.stderr or p.stdout or f"exit {p.returncode}").strip()
        except Exception as exc:
            return False, str(exc)

    raspi_gpio = shutil.which("raspi-gpio")
    if raspi_gpio:
        try:
            p = subprocess.run(
                [raspi_gpio, "set", str(gpio), "op", "dh" if high else "dl"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if p.returncode == 0:
                return True, None
            return False, (p.stderr or p.stdout or f"exit {p.returncode}").strip()
        except Exception as exc:
            return False, str(exc)

    return False, "neither pinctrl nor raspi-gpio is installed"


def prepare_dht_power(cold_cycle=False):
    """Ensure the GPIO17-powered DHT rail is on before discovery."""
    heading("DHT22 SWITCHED POWER")
    print(
        f"DHT VCC rail: GPIO{DHT_POWER_GPIO} / physical pin "
        f"{DHT_POWER_PHYSICAL_PIN}"
    )

    if cold_cycle:
        print(f"Cold-cycling DHT power OFF for {DHT_POWER_OFF_S:.1f} s...")
        ok, error = set_gpio_output(DHT_POWER_GPIO, False)
        if not ok:
            print(f"FAIL: could not switch DHT power off: {error}")
            return False
        time.sleep(DHT_POWER_OFF_S)

    print("Driving DHT power rail HIGH...")
    ok, error = set_gpio_output(DHT_POWER_GPIO, True)
    if not ok:
        print(f"FAIL: could not switch DHT power on: {error}")
        return False

    time.sleep(DHT_POWER_STARTUP_S)
    state = gpio_state(DHT_POWER_GPIO)
    print(f"Power GPIO state: {state or 'unavailable'}")
    return True


def print_header():
    heading("RASPBERRY PI 40-PIN HEADER")

    print(
        f"{'PHYS':>4}  {'BCM':>5}  {'LABEL':<8} "
        f"{'LEVEL':<5}  STATE"
    )
    print("-" * 78)

    states = {}

    for phys in range(1, 41):
        label, gpio = HEADER[phys]

        if gpio is None:
            print(
                f"{phys:>4}  {'-':>5}  {label:<8} "
                f"{'-':<5}  {label}"
            )
            continue

        state = gpio_state(gpio)
        states[gpio] = state

        level = gpio_level(state)

        if state:
            # Avoid printing the GPIO number twice where possible
            compact = state
        else:
            compact = "unable to read GPIO state"

        print(
            f"{phys:>4}  {gpio:>5}  {label:<8} "
            f"{level:<5}  {compact}"
        )

    return states


# ---------------------------------------------------------------------------
# DHT22
# ---------------------------------------------------------------------------

def shorten_error(exc):
    text = str(exc).strip().replace("\n", " ")
    if len(text) > 90:
        text = text[:87] + "..."
    return text


def test_dht_gpio(gpio, tries=2):
    import board
    import adafruit_dht

    pin = getattr(board, f"D{gpio}", None)

    if pin is None:
        return None, f"board.D{gpio} unavailable"

    sensor = None
    errors = []

    try:
        sensor = adafruit_dht.DHT22(
            pin,
            use_pulseio=False,
        )

        for attempt in range(1, tries + 1):
            try:
                temperature = sensor.temperature
                humidity = sensor.humidity

                if (
                    temperature is not None
                    and humidity is not None
                    and -50.0 <= temperature <= 100.0
                    and 0.0 <= humidity <= 100.0
                ):
                    return (
                        {
                            "gpio": gpio,
                            "physical": GPIO_TO_PHYS.get(gpio),
                            "temperature": float(temperature),
                            "humidity": float(humidity),
                        },
                        None,
                    )

                errors.append(
                    f"attempt {attempt}: implausible/empty reading "
                    f"T={temperature}, RH={humidity}"
                )

            except Exception as exc:
                errors.append(
                    f"attempt {attempt}: {shorten_error(exc)}"
                )

            if attempt < tries:
                # DHT22 should not be hammered faster than ~2 seconds.
                time.sleep(2.1)

    except Exception as exc:
        errors.append(shorten_error(exc))

    finally:
        if sensor is not None:
            try:
                sensor.exit()
            except Exception:
                pass

    return None, "; ".join(errors)


def dht_candidates(states, preferred=None):
    candidates = []

    if preferred:
        for gpio in preferred:
            if gpio not in candidates:
                candidates.append(gpio)

    for gpio in DHT_PRIORITY:
        if gpio not in candidates:
            candidates.append(gpio)

    for gpio in sorted(GPIO_TO_PHYS):
        if gpio not in candidates:
            candidates.append(gpio)

    actual = []
    for gpio in candidates:
        if gpio in PROTECTED_DHT_GPIOS:
            continue
        state = states.get(gpio)
        if gpio_is_input(state):
            actual.append(gpio)

    return actual


def scan_dhts(states, passes=3, preferred=None):
    """Search all safe GPIOs and return every DHT22 that answers.

    One transaction is attempted per GPIO per pass.  This avoids the old
    behaviour of sleeping for ~2 seconds on every empty GPIO while still giving
    timing-sensitive DHT sensors a second discovery pass.
    """
    heading("DHT22 GPIO SEARCH")

    try:
        import board          # noqa: F401
        import adafruit_dht   # noqa: F401
    except ImportError as exc:
        print(f"FAIL: missing Python module: {exc}")
        return {
            "ok": False,
            "sensors": [],
            "reason": f"missing module: {exc}",
        }

    candidates = dht_candidates(states, preferred=preferred)

    print("DHT timing will be attempted on these currently-input GPIOs:")
    if candidates:
        print(
            "  "
            + ", ".join(
                f"GPIO{g} (physical pin {GPIO_TO_PHYS.get(g, '?')})"
                for g in candidates
            )
        )
    else:
        print("  none")

    print()
    print("GPIO0/1 skipped: HAT EEPROM pins")
    print("GPIO2/3 skipped: I2C SDA/SCL")
    print(
        f"GPIO{DHT_POWER_GPIO} skipped: switched DHT VCC "
        f"(physical pin {DHT_POWER_PHYSICAL_PIN})"
    )
    print()

    if not candidates:
        return {
            "ok": False,
            "sensors": [],
            "reason": "no safe input GPIOs available to probe",
        }

    found = {}
    last_errors = {}

    for pass_number in range(1, max(1, passes) + 1):
        if passes > 1:
            print(f"Discovery pass {pass_number}/{passes}")

        for gpio in candidates:
            # Once found, do not keep disturbing that sensor during discovery.
            if gpio in found:
                continue

            phys = GPIO_TO_PHYS.get(gpio, "?")
            print(
                f"Testing GPIO{gpio:>2} / physical pin {phys:>2} ... ",
                end="",
                flush=True,
            )

            result, error = test_dht_gpio(gpio, tries=1)
            if result:
                found[gpio] = result
                print(
                    f"FOUND  {result['temperature']:.2f} C, "
                    f"{result['humidity']:.2f} %"
                )
            else:
                last_errors[gpio] = error
                print(f"no response [{error}]")

        if pass_number < passes:
            time.sleep(2.2)

    sensors = [found[gpio] for gpio in candidates if gpio in found]

    print()
    if sensors:
        print(f"Detected {len(sensors)} DHT22 sensor(s):")
        for index, sensor in enumerate(sensors, start=1):
            print(
                f"  DHT22[{index}] -> GPIO{sensor['gpio']} / "
                f"physical pin {sensor['physical']}: "
                f"{sensor['temperature']:.2f} C, "
                f"{sensor['humidity']:.2f} %"
            )
        return {
            "ok": True,
            "sensors": sensors,
        }

    return {
        "ok": False,
        "sensors": [],
        "reason": "no valid DHT22 response on scanned GPIOs",
    }


# ---------------------------------------------------------------------------
# BME280
# ---------------------------------------------------------------------------

def test_bme280():
    heading("BME280 / I2C TEST")

    try:
        import board
    except ImportError as exc:
        print(f"FAIL: missing Python module: {exc}")
        return {"ok": False, "reason": f"missing module: {exc}"}

    i2c = None

    try:
        i2c = board.I2C()

        start = time.monotonic()
        while not i2c.try_lock():
            if time.monotonic() - start > 5:
                raise RuntimeError("could not acquire I2C bus lock")
            time.sleep(0.05)

        try:
            addresses = i2c.scan()
        finally:
            i2c.unlock()

        address_text = (
            ", ".join(f"0x{x:02X}" for x in addresses)
            if addresses else "none"
        )
        print(f"I2C devices found: {address_text}")

        bme_addresses = [addr for addr in (0x76, 0x77) if addr in addresses]
        if not bme_addresses:
            print("BME280 not detected at 0x76 or 0x77 (left disabled)")
            return {
                "ok": False,
                "reason": "no device detected at I2C address 0x76 or 0x77",
                "addresses": addresses,
            }

        try:
            import adafruit_bme280.basic as adafruit_bme280
        except ImportError as exc:
            print("FAIL: BME280 is present on I2C, but its Python driver is missing.")
            print(
                "Install with: sudo pip3 install "
                "adafruit-circuitpython-bme280 --break-system-packages"
            )
            return {
                "ok": False,
                "reason": f"missing adafruit_bme280 driver: {exc}",
                "addresses": addresses,
            }

        last_error = None
        for address in bme_addresses:
            try:
                bme = adafruit_bme280.Adafruit_BME280_I2C(i2c, address=address)
                bme.sea_level_pressure = 1013.25
                time.sleep(0.5)

                temperature = float(bme.temperature)
                humidity = float(bme.relative_humidity)
                pressure = float(bme.pressure)
                altitude = float(bme.altitude)

                print()
                print(f"BME280 detected at 0x{address:02X}")
                print(f"  Temperature : {temperature:.2f} C")
                print(f"  Humidity    : {humidity:.2f} %")
                print(f"  Pressure    : {pressure:.2f} hPa")
                print(f"  Altitude    : {altitude:.2f} m (derived)")

                return {
                    "ok": True,
                    "model": "BME280",
                    "address": address,
                    "temperature": temperature,
                    "humidity": humidity,
                    "pressure": pressure,
                    "altitude": altitude,
                    "addresses": addresses,
                }

            except Exception as exc:
                last_error = exc
                print(
                    f"Device at 0x{address:02X} did not produce "
                    f"a valid BME280 reading: {exc}"
                )

        return {
            "ok": False,
            "reason": str(last_error),
            "addresses": addresses,
        }

    except Exception as exc:
        print(f"FAIL: {exc}")
        return {"ok": False, "reason": str(exc)}

    finally:
        if i2c is not None:
            try:
                i2c.deinit()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Camera
# ---------------------------------------------------------------------------

def camera_with_picamera2(output):
    try:
        from picamera2 import Picamera2
    except ImportError:
        return None

    try:
        info = Picamera2.global_camera_info()

        print(f"Picamera2 reports {len(info)} camera(s)")

        for n, camera in enumerate(info):
            print(f"  Camera {n}: {camera}")

        if not info:
            return {
                "ok": False,
                "detected": False,
                "reason": "Picamera2 found no cameras",
            }

        model = info[0].get("Model", "unknown")
        camera = Picamera2(0)

        config = camera.create_still_configuration(
            main={"size": (1280, 720)}
        )

        camera.configure(config)
        camera.start()

        # Let auto exposure/white balance settle.
        time.sleep(2)

        camera.capture_file(str(output))

        camera.stop()
        camera.close()

        if output.exists() and output.stat().st_size > 0:
            return {
                "ok": True,
                "detected": True,
                "model": model,
                "method": "Picamera2",
                "file": str(output),
                "size": output.stat().st_size,
            }

        return {
            "ok": False,
            "reason": "capture completed but image file is empty",
        }

    except Exception as exc:
        message = str(exc)
        busy = (
            "in use by another process" in message.lower()
            or "did not complete" in message.lower()
        )
        return {
            "ok": False,
            "detected": bool(locals().get("info")),
            "busy": busy,
            "model": locals().get("model", "unknown"),
            "reason": f"Picamera2: {message}",
        }


def camera_with_cli(output):
    binaries = [
        "rpicam-still",
        "libcamera-still",
    ]

    for name in binaries:
        binary = shutil.which(name)

        if not binary:
            continue

        print(f"Trying {name}...")

        try:
            if output.exists():
                output.unlink()

            proc = subprocess.run(
                [
                    binary,
                    "-n",
                    "-t",
                    "2000",
                    "-o",
                    str(output),
                ],
                capture_output=True,
                text=True,
                timeout=15,
            )

            if (
                proc.returncode == 0
                and output.exists()
                and output.stat().st_size > 0
            ):
                return {
                    "ok": True,
                    "method": name,
                    "file": str(output),
                    "size": output.stat().st_size,
                }

            error = (
                proc.stderr.strip()
                or proc.stdout.strip()
                or f"exit code {proc.returncode}"
            )

            print(f"{name} failed: {error}")

        except Exception as exc:
            print(f"{name} failed: {exc}")

    return {
        "ok": False,
        "reason": (
            "no working Picamera2, rpicam-still, "
            "or libcamera-still capture"
        ),
    }


def test_camera():
    heading("CAMERA TEST")

    output = Path("/tmp/weather_camera_test.jpg")

    try:
        if output.exists():
            output.unlink()
    except Exception:
        pass

    result = camera_with_picamera2(output)

    if result is None or not result.get("ok"):
        if result is not None:
            print(
                f"Picamera2 capture failed: "
                f"{result.get('reason', 'unknown error')}"
            )

        result = camera_with_cli(output)

    if result.get("ok"):
        print()
        print("Camera capture successful")
        print(f"  Method : {result['method']}")
        print(f"  File   : {result['file']}")
        print(f"  Size   : {result['size']:,} bytes")
    else:
        print()
        print(
            f"FAIL: {result.get('reason', 'unknown camera error')}"
        )

    return result


# ---------------------------------------------------------------------------
# Hardware config + service control
# ---------------------------------------------------------------------------

def service_is_active():
    try:
        result = subprocess.run(
            ["systemctl", "is-active", "--quiet", WEATHER_SERVICE],
            timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def systemctl(action):
    command = ["sudo", "systemctl", action, WEATHER_SERVICE]
    try:
        result = subprocess.run(command, timeout=30)
        return result.returncode == 0
    except Exception as exc:
        print(f"WARNING: {' '.join(command)} failed: {exc}")
        return False


def write_hardware_config(dht_result, bme_result, camera_result, path):
    """Atomically write the hardware map consumed by weather_station.py."""
    payload = {
        "version": 3,
        "updated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dht22": [
            {
                "gpio": int(sensor["gpio"]),
                "physical_pin": int(sensor["physical"]),
            }
            for sensor in dht_result.get("sensors", [])
        ],
        "dht_power": {
            "gpio": DHT_POWER_GPIO,
            "physical_pin": DHT_POWER_PHYSICAL_PIN,
        },
        "bme280": (
            {
                "address": int(bme_result["address"]),
                "model": "BME280",
            }
            if bme_result.get("ok")
            else None
        ),
        "camera": {
            "detected": bool(
                camera_result.get("ok") or camera_result.get("detected")
            ),
            "model": camera_result.get("model"),
        },
    }

    path = Path(path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")

    with temp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temp, path)
    return payload


def print_hardware_config(payload, path):
    heading("WEATHER STATION HARDWARE CONFIG")
    print(f"Wrote: {Path(path).expanduser()}")

    dhts = payload.get("dht22", [])
    if dhts:
        for index, sensor in enumerate(dhts, start=1):
            print(
                f"  DHT22[{index}] : GPIO{sensor['gpio']} / "
                f"physical pin {sensor['physical_pin']}"
            )
    else:
        print("  DHT22       : none detected")

    dht_power = payload.get("dht_power", {})
    print(
        f"  DHT power   : GPIO{dht_power.get('gpio', DHT_POWER_GPIO)} / "
        f"physical pin {dht_power.get('physical_pin', DHT_POWER_PHYSICAL_PIN)}"
    )

    bme = payload.get("bme280")
    if bme:
        print(f"  BME280      : I2C 0x{bme['address']:02X}")
    else:
        print("  BME280      : disabled/not detected")

    camera = payload.get("camera", {})
    if camera.get("detected"):
        print(f"  Camera      : {camera.get('model') or 'detected'}")
    else:
        print("  Camera      : not detected")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(dht, bme, camera):
    heading("FINAL DIAGNOSTIC SUMMARY")

    sensors = dht.get("sensors", [])
    if sensors:
        for index, sensor in enumerate(sensors, start=1):
            print(
                f"DHT22[{index}] : PASS  "
                f"GPIO{sensor['gpio']} / physical pin "
                f"{sensor['physical']}   "
                f"{sensor['temperature']:.2f} C   "
                f"{sensor['humidity']:.2f} %"
            )
    else:
        print(
            "DHT22      : NOT FOUND  "
            f"{dht.get('reason', 'no valid response')}"
        )

    if bme.get("ok"):
        print(
            "BME280     : PASS  "
            f"0x{bme['address']:02X}   "
            f"{bme['temperature']:.2f} C   "
            f"{bme['humidity']:.2f} %   "
            f"{bme['pressure']:.2f} hPa   "
            f"alt={bme['altitude']:.2f} m"
        )
    else:
        print(
            "BME280     : NOT FOUND / DISABLED  "
            f"{bme.get('reason', 'no BME280 detected')}"
        )

    if camera.get("ok"):
        model = camera.get("model", "camera")
        print(
            f"CAMERA     : PASS  {model}  "
            f"{camera['method']}   {camera['file']}"
        )
    elif camera.get("detected"):
        print(
            "CAMERA     : DETECTED but capture unavailable  "
            f"{camera.get('model', 'unknown')}  "
            f"{camera.get('reason', '')}"
        )
    else:
        print(
            "CAMERA     : NOT DETECTED  "
            f"{camera.get('reason', 'unknown reason')}"
        )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Weather-station hardware diagnostic and automatic hardware "
            "configuration"
        )
    )

    parser.add_argument(
        "--dht-pin",
        type=int,
        action="append",
        default=None,
        help=(
            "BCM GPIO to try early in the DHT search. May be supplied more "
            "than once. Discovery still scans all safe GPIOs."
        ),
    )

    parser.add_argument(
        "--dht-tries",
        type=int,
        default=3,
        help=(
            "Number of full DHT discovery passes (default: 3). Each GPIO is "
            "read at most once per pass."
        ),
    )

    parser.add_argument(
        "--dht-power-cycle",
        action="store_true",
        help=(
            "cold-cycle the GPIO17 DHT VCC rail before discovery "
            f"({DHT_POWER_OFF_S:.0f}s off, {DHT_POWER_STARTUP_S:.0f}s startup)"
        ),
    )

    parser.add_argument(
        "--no-camera",
        action="store_true",
        help="skip camera capture test",
    )

    parser.add_argument(
        "--no-apply",
        action="store_true",
        help="diagnose only; do not update weather_hardware.json",
    )

    parser.add_argument(
        "--config",
        default=str(HARDWARE_CONFIG_PATH),
        help=f"hardware config path (default: {HARDWARE_CONFIG_PATH})",
    )

    parser.add_argument(
        "--no-service-control",
        action="store_true",
        help=(
            f"do not stop/restart {WEATHER_SERVICE} around discovery"
        ),
    )

    args = parser.parse_args()

    print()
    print("Weather Station Hardware Diagnostic")
    print("-----------------------------------")
    print(f"Python: {sys.version.split()[0]}")
    print(f"Host:   {os.uname().nodename}")
    print()

    service_was_active = False
    service_stopped = False

    if not args.no_service_control:
        service_was_active = service_is_active()
        if service_was_active:
            print(
                f"{WEATHER_SERVICE} is running; stopping it temporarily "
                "so GPIO/camera discovery is exclusive."
            )
            service_stopped = systemctl("stop")
            if not service_stopped:
                print(
                    "WARNING: could not stop the weather station service. "
                    "Sensor/camera discovery may be unreliable."
                )

    try:
        if not prepare_dht_power(cold_cycle=args.dht_power_cycle):
            print(
                "WARNING: DHT power rail could not be prepared; DHT discovery "
                "will probably fail."
            )

        states = print_header()

        dht = scan_dhts(
            states,
            passes=max(1, args.dht_tries),
            preferred=args.dht_pin,
        )

        bme = test_bme280()

        if args.no_camera:
            camera = {
                "ok": False,
                "detected": False,
                "reason": "test skipped",
            }
        else:
            camera = test_camera()

        print_summary(dht, bme, camera)

        if not args.no_apply:
            payload = write_hardware_config(
                dht,
                bme,
                camera,
                args.config,
            )
            print_hardware_config(payload, args.config)
            print()
            print(
                "weather_station.py will use these assignments automatically "
                "the next time it starts."
            )

    finally:
        if (
            not args.no_service_control
            and service_was_active
            and service_stopped
        ):
            print()
            print(f"Restarting {WEATHER_SERVICE}...")
            if systemctl("start"):
                print(f"{WEATHER_SERVICE} restarted.")
            else:
                print(
                    f"WARNING: failed to restart {WEATHER_SERVICE}; "
                    "start it manually with:"
                )
                print(f"  sudo systemctl start {WEATHER_SERVICE}")


if __name__ == "__main__":
    main()
