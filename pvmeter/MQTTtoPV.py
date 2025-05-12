#!/usr/bin/env python

"""
This script adds an external PV-meter to the Victron VenusOS and the graphical user-interface

Parts of this code are based on the work of Ralf Zimmermann (mail@ralfzimmermann.de) in 2020.
The orginal code and its documentation can be found on: https://github.com/RalfZim/venus.dbus-fronius-smartmeter
Used https://github.com/victronenergy/velib_python/blob/master/dbusdummyservice.py as basis for this service.
"""

"""
Short instruction to install this script as a service:
1. Copy all files to /data/pvmeter on the VenusOS (CerboGX or RaspberryPi)

2. set permissions
chmod 755 /data/pvmeter/service/run
chmod 744 /data/pvmeter/kill_me.sh

3. install / uninstall service
bash -x /data/pvmeter/install.sh
bash -x /data/pvmeter/uninstall.sh

4. check status
svstat /service/pvmeter

5. in case of errors debug:
python /data/pvmeter/MQTTtoPV.py

If paho-mqtt is not installed, use this to install the dependencies:
python -m ensurepip --upgrade
pip install paho-mqtt
"""
try:
  import gobject  # Python 2.x
except:
  from gi.repository import GLib as gobject # Python 3.x
import platform
import logging
import time
import sys
import json
import os
import paho.mqtt.client as mqtt
import configparser  # for config/ini file
try:
  import thread   # for daemon = True  / Python 2.x
except:
  import _thread as thread   # for daemon = True  / Python 3.x

# our own packages
sys.path.insert(1, os.path.join(os.path.dirname(__file__), '../ext/velib_python'))
from vedbus import VeDbusService


# get values from config.ini file
try:
    config_file = (os.path.dirname(os.path.realpath(__file__))) + "/config.ini"
    if os.path.exists(config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        if config["MQTT"]["broker_address"] == "IP_ADDR_OR_FQDN":
            print('ERROR:The "config.ini" is using invalid default values like IP_ADDR_OR_FQDN. The driver restarts in 60 seconds.')
            sleep(60)
            sys.exit()
    else:
        print('ERROR:The "' + config_file + '" is not found. Did you copy or rename the "config.sample.ini" to "config.ini"? The driver restarts in 60 seconds.')
        sleep(60)
        sys.exit()

except Exception:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    file = exception_traceback.tb_frame.f_code.co_filename
    line = exception_traceback.tb_lineno
    print(f"Exception occurred: {repr(exception_object)} of type {exception_type} in {file} line #{line}")
    print("ERROR:The driver restarts in 60 seconds.")
    sleep(60)
    sys.exit()

path_UpdateIndex = '/UpdateIndex'

# Variablen setzen
verbunden = 0
power = None
voltage = None
current = None
frequency = None
energy_180 = None
energy_280 = None
dbusservice = None

# MQTT Abfragen:

def on_disconnect(client, userdata, rc):
    global verbunden
    print("MQTT client Got Disconnected")
    if rc != 0:
        print('Unexpected MQTT disconnection. Will auto-reconnect')

    else:
        print('rc value:' + str(rc))

    try:
        print("Trying to Reconnect")
        client.connect(host=config["MQTT"]["broker_address"], port=int(config["MQTT"]["broker_port"]))
        verbunden = 1
    except Exception as e:
        logging.exception("Fehler beim reconnecten mit Broker")
        print("Error in Retrying to Connect with Broker")
        verbunden = 0
        print(e)

def on_connect(client, userdata, flags, rc):
        global verbunden
        if rc == 0:
            print("Connected to MQTT Broker!")
            verbunden = 1
            ok = client.subscribe(config["MQTT"]["topic"]+"/#", 0)
            print("subscribed to "+config["MQTT"]["topic"]+" ok="+str(ok))
        else:
            print("Failed to connect, return code %d\n" % rc)


def on_message(client, userdata, msg):

    try:

        global power
        global voltage
        global current
        global frequency
        global energy_180
        global energy_280

        if msg.topic == config["MQTT"]["topic"] + "/power":
            # power in W
            power = -1 * float(msg.payload)
        elif msg.topic == config["MQTT"]["topic"] + "/voltage":
            # Voltage in V
            voltage = float(msg.payload)
        elif msg.topic == config["MQTT"]["topic"] + "/current":
            # Current in A
            current = -1 * float(msg.payload)
        elif msg.topic == config["MQTT"]["topic"] + "/frequency":
            # Frequency in Hz
            frequency = float(msg.payload)
        elif msg.topic == config["MQTT"]["topic"] + "/energy_180":
            # Energy in kWh
            energy_180 = float(msg.payload)/1000.0
        elif msg.topic == config["MQTT"]["topic"] + "/energy_280":
            # Energy in kWh
            energy_280 = float(msg.payload)/1000.0

        dbusservice._update()

    except Exception as e:
        logging.exception("Programm MQTTtoPV ist abgestuerzt. (during on_message function)")
        print(e)
        print("Im MQTTtoPV Programm ist etwas beim Auslesen der Nachrichten schief gegangen")

class DbusDummyService:
  def __init__(self, servicename, deviceinstance, paths, productname=config["DEFAULT"]["device_name"], connection=config["MQTT"]["connection_name"]):
    self._dbusservice = VeDbusService(servicename)
    self._paths = paths

    logging.debug("%s /DeviceInstance = %d" % (servicename, deviceinstance))

    # Create the management objects, as specified in the ccgx dbus-api document
    self._dbusservice.add_path('/Mgmt/ProcessName', __file__)
    self._dbusservice.add_path('/Mgmt/ProcessVersion', 'Unkown version, and running on Python ' + platform.python_version())
    self._dbusservice.add_path('/Mgmt/Connection', connection)

    # Create the mandatory objects
    self._dbusservice.add_path('/DeviceInstance', deviceinstance)
    self._dbusservice.add_path('/ProductId', 0xFFFF) # id assigned by Victron Support from SDM630v2.py
    self._dbusservice.add_path('/ProductName', productname)
    #self._dbusservice.add_path("/CustomName", customname)
    self._dbusservice.add_path('/FirmwareVersion', 0.1)
    #self._dbusservice.add_path('/HardwareVersion', 0)
    self._dbusservice.add_path('/Connected', 1)
    self._dbusservice.add_path('/Latency', 0)
    self._dbusservice.add_path('/ErrorCode', 0)
    self._dbusservice.add_path('/Position', int(config["PV"]["position"])) # 0=AC input 1, 1=AC output, 2=AC input 2
    self._dbusservice.add_path("/StatusCode", 0)
    # 0=Startup 0; 1=Startup 1; 2=Startup 2; 3=Startup 3; 4=Startup 4; 5=Startup 5; 6=Startup 6; 7=Running; 8=Standby; 9=Boot loading; 10=Error

    for path, settings in self._paths.items():
      self._dbusservice.add_path(
        path, settings['initial'], gettextcallback=settings['textformat'], writeable=True, onchangecallback=self._handlechangedvalue)

    # register VeDbusService after all paths where added
    self._dbusservice.register()

    # now _update ios called from on_message:
    #gobject.timeout_add(1000, self._update) # pause 1000ms before the next request

  def _update(self):

    if not power is None: self._dbusservice['/Ac/Power'] = power
    if not current is None:
        self._dbusservice['/Ac/Current'] = current
    else:
        if not power is None: self._dbusservice['/Ac/Current'] = power/float(config["DEFAULT"]["voltage"])
    if not voltage is None:
        self._dbusservice['/Ac/Voltage'] = voltage
    else:
        self._dbusservice['/Ac/Voltage'] = float(config["DEFAULT"]["voltage"])
    if not energy_280 is None:
        self._dbusservice['/Ac/Energy/Forward'] = energy_280
    else:
        self._dbusservice['/Ac/Energy/Forward'] = 0
    #if not xxx is None: self._dbusservice['/Ac/Energy/Forward'] =  xxx

    if not power is None: self._dbusservice['/Ac/L1/Power'] = power
    if not current is None:
        self._dbusservice['/Ac/L1/Current'] = current
    else:
        if not power is None: self._dbusservice['/Ac/L1/Current'] = power/float(config["DEFAULT"]["voltage"])
    if not voltage is None:
        self._dbusservice['/Ac/L1/Voltage'] = voltage
    else:
        self._dbusservice['/Ac/L1/Voltage'] = float(config["DEFAULT"]["voltage"])
    if not frequency is None:
        self._dbusservice['/Ac/L1/Frequency'] = frequency
    else:
        self._dbusservice['/Ac/L1/Frequency'] = float(config["DEFAULT"]["frequency"])
    if not energy_280 is None:
        self._dbusservice['/Ac/L1/Energy/Forward'] = energy_280
    else:
        self._dbusservice['/Ac/L1/Energy/Forward'] = 0
    #if not xxx is None: self._dbusservice['/Ac/L1/Energy/Forward'] =  xxx

    # increment UpdateIndex - to show that new data is available
    index = self._dbusservice[path_UpdateIndex] + 1  # increment index
    if index > 255:   # maximum value of the index
      index = 0       # overflow from 255 to 0
    self._dbusservice[path_UpdateIndex] = index

    # is only displayed for Fronius inverters (product ID 0xA142) in GUI but displayed in VRM portal
    # if power above 10 W, set status code to 7 (running)
    # 0=Startup 0; 1=Startup 1; 2=Startup 2; 3=Startup 3; 4=Startup 4; 5=Startup 5; 6=Startup 6; 7=Running; 8=Standby; 9=Boot loading; 10=Error
    if not power is None:
        if self._dbusservice["/Ac/Power"] >= 10:
            if self._dbusservice["/StatusCode"] != 7:
                self._dbusservice["/StatusCode"] = 7
        # else set status code to 8 (standby)
        else:
            if self._dbusservice["/StatusCode"] != 8:
                self._dbusservice["/StatusCode"] = 8
    else:
        if self._dbusservice["/StatusCode"] != 8:
            self._dbusservice["/StatusCode"] = 8
    self._lastUpdate = time.time()

    return True

  def _handlechangedvalue(self, path, value):
    logging.debug("someone else updated %s to %s" % (path, value))
    return True # accept the change

def main():
  #logging.basicConfig(level=logging.INFO) # use .INFO for less, .DEBUG for more logging
  logging.basicConfig(
      format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
      datefmt="%Y-%m-%d %H:%M:%S",
      level=logging.INFO,
      # level=logging.DEBUG,
      handlers=[
          logging.FileHandler(f"{(os.path.dirname(os.path.realpath(__file__)))}/current.log"),
          logging.StreamHandler(),
      ],
  )
  thread.daemon = True # allow the program to quit

  from dbus.mainloop.glib import DBusGMainLoop
  # Have a mainloop, so we can send/receive asynchronous calls to and from dbus
  DBusGMainLoop(set_as_default=True)

  # formatting
  def _kwh(p, v): return (str(round(v, 2)) + 'kWh')
  def _wh(p, v): return (str(round(v, 2)) + 'Wh')
  def _a(p, v): return (str(round(v, 2)) + 'A')
  def _w(p, v): return (str(int(round(v, 0))) + 'W')
  def _v(p, v): return (str(round(v, 1)) + 'V')
  def _hz(p, v): return (str(round(v, 2)) + 'Hz')
  def _n(p, v): return str("%i" % v)

  global dbusservice
  dbusservice = DbusDummyService(
    servicename='com.victronenergy.pvinverter.mqtt_pv_' + str(config["DEFAULT"]["device_instance"]),
    deviceinstance=int(config["DEFAULT"]["device_instance"]),
    #customname=config["DEFAULT"]["device_name"],
    paths={
      '/Ac/Power': {'initial': None, 'textformat': _w},
      '/Ac/Current': {'initial': None, 'textformat': _a},
      '/Ac/Voltage': {'initial': None, 'textformat': _v},
      '/Ac/Energy/Forward': {'initial': None, 'textformat': _kwh},
      '/Ac/MaxPower': {'initial': int(config["PV"]["max"]), 'textformat': _w},
      '/Ac/Position': {'initial': int(config["PV"]["position"]), 'textformat': _n},
      '/Ac/StatusCode': {'initial': 0, 'textformat': _n},
      '/UpdateIndex': {'initial': 0, 'textformat': _n},
      '/Ac/L1/Power': {'initial': None, 'textformat': _w},
      '/Ac/L1/Current': {'initial': None, 'textformat': _a},
      '/Ac/L1/Voltage': {'initial': None, 'textformat': _v},
      '/Ac/L1/Frequency': {'initial': None, 'textformat': _hz},
      '/Ac/L1/Energy/Forward': {'initial': None, 'textformat': _kwh},
      #'/Ac/L2/Power': {'initial': None, 'textformat': _w},
      #'/Ac/L2/Current': {'initial': None, 'textformat': _a},
      #'/Ac/L2/Voltage': {'initial': None, 'textformat': _v},
      #'/Ac/L2/Frequency': {'initial': None, 'textformat': _hz},
      #'/Ac/L2/Energy/Forward': {'initial': None, 'textformat': _kwh},
      #'/Ac/L3/Power': {'initial': None, 'textformat': _w},
      #'/Ac/L3/Current': {'initial': None, 'textformat': _a},
      #'/Ac/L3/Voltage': {'initial': None, 'textformat': _v},
      #'/Ac/L3/Frequency': {'initial': None, 'textformat': _hz},
      #'/Ac/L3/Energy/Forward': {'initial': None, 'textformat': _kwh},
    })

  logging.info('Connected to dbus, and switching over to gobject.MainLoop() (= event based)')
  mainloop = gobject.MainLoop()
  mainloop.run()

# Konfiguration MQTT
client = mqtt.Client(config["MQTT"]["mqtt_name"]) # create new instance
if ((config["MQTT"]["broker_user"] != "") and (config["MQTT"]["broker_password"] != "")):
    client.username_pw_set(config["MQTT"]["broker_user"], config["MQTT"]["broker_password"])
client.on_disconnect = on_disconnect
client.on_connect = on_connect
client.on_message = on_message
client.connect(host=config["MQTT"]["broker_address"], port=int(config["MQTT"]["broker_port"]))  # connect to broker

client.loop_start()

if __name__ == "__main__":
  main()
