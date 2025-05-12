#!/usr/bin/env python

"""
This script adds an external Grid-Meter to the Victron VenusOS and the graphical user-interface

Parts of this code are based on the work of Ralf Zimmermann (mail@ralfzimmermann.de) in 2020.
The orginal code and its documentation can be found on: https://github.com/RalfZim/venus.dbus-fronius-smartmeter
Used https://github.com/victronenergy/velib_python/blob/master/dbusdummyservice.py as basis for this service.
"""

"""
Short instruction to install this script as a service:
1. Copy all files to /data/gridmeter on the VenusOS (CerboGX or RaspberryPi)

2. set permissions
chmod 755 /data/gridmeter/service/run
chmod 744 /data/gridmeter/kill_me.sh

3. install / uninstall service
bash -x /data/gridmeter/install.sh
bash -x /data/gridmeter/uninstall.sh

4. check status
svstat /service/gridmeter

5. in case of errors debug:
python /data/gridmeter/MQTTtoGridMeter.py

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
try:
  import thread   # for daemon = True  / Python 2.x
except:
  import _thread as thread   # for daemon = True  / Python 3.x

# our own packages
sys.path.insert(1, os.path.join(os.path.dirname(__file__), '../ext/velib_python'))
from vedbus import VeDbusService

path_UpdateIndex = '/UpdateIndex'

# Device Setup
METER_PRODUCT_NAME = 'Grid meter'
METER_CONNECTION_NAME = 'LCARS/MQTT'

# MQTT Setup
MQTT_BROKER_ADDRESS = "192.168.0.24"
MQTTNAME = "K4_VenusOS_MQTTMeter"
MQTT_PATH = "k4/power/meter"
MQTT_USERNAME = ""
MQTT_PASSWORD = ""

# Variablen setzen
verbunden = 0
power_sum = None
power_l1 = None
power_l2 = None
power_l3 = None
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
        client.connect(MQTT_BROKER_ADDRESS)
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
            ok = client.subscribe(MQTT_PATH+"/#", 0)
            print("subscribed to "+MQTT_PATH+" ok="+str(ok))
        else:
            print("Failed to connect, return code %d\n" % rc)


def on_message(client, userdata, msg):

    try:

        global power_sum, power_l1, power_l2, power_l3, energy_180, energy_280, dbusservice
        
        if msg.topic == MQTT_PATH + "/power":
            # power in W
            power_sum = float(msg.payload)
            power_l1 = power_sum/3;
            power_l2 = power_sum/3;
            power_l3 = power_sum/3;
        elif msg.topic == MQTT_PATH + "/p_l1":
            power_l1 = float(msg.payload)
        elif msg.topic == MQTT_PATH + "/p_l2":
            power_l2 = float(msg.payload)
        elif msg.topic == MQTT_PATH + "/p_l3":
            power_l3 = float(msg.payload)
        elif msg.topic == MQTT_PATH + "/180":
            # energy in kWh
            energy_180 = round(float(msg.payload) / 1000, 3)
        elif msg.topic == MQTT_PATH + "/280":
            # energy in kWh
            energy_280 = round(float(msg.payload) / 1000, 3)

        dbusservice._update()

    except Exception as e:
        logging.exception("Programm MQTTtoMeter ist abgestuerzt. (during on_message function)")
        print(e)
        print("Im MQTTtoMeter Programm ist etwas beim auslesen der Nachrichten schief gegangen")




class DbusDummyService:
  def __init__(self, servicename, deviceinstance, paths, productname=METER_PRODUCT_NAME, connection=METER_CONNECTION_NAME):
    self._dbusservice = VeDbusService(servicename)
    self._paths = paths

    logging.debug("%s /DeviceInstance = %d" % (servicename, deviceinstance))

    # Create the management objects, as specified in the ccgx dbus-api document
    self._dbusservice.add_path('/Mgmt/ProcessName', __file__)
    self._dbusservice.add_path('/Mgmt/ProcessVersion', 'Unkown version, and running on Python ' + platform.python_version())
    self._dbusservice.add_path('/Mgmt/Connection', connection)

    # Create the mandatory objects
    self._dbusservice.add_path('/DeviceInstance', deviceinstance)
    self._dbusservice.add_path('/ProductId', 45069) # 45069 = value used in ac_sensor_bridge.cpp of dbus-cgwacs, found on https://www.sascha-curth.de/projekte/005_Color_Control_GX.html#experiment - should be an ET340 Engerie Meter
    self._dbusservice.add_path('/DeviceType', 345) # found on https://www.sascha-curth.de/projekte/005_Color_Control_GX.html#experiment - should be an ET340 Engerie Meter
    self._dbusservice.add_path('/Role', 'grid')


    self._dbusservice.add_path('/ProductName', productname)
    self._dbusservice.add_path('/FirmwareVersion', 0.1)
    self._dbusservice.add_path('/HardwareVersion', 0)
    self._dbusservice.add_path('/Connected', 1)
    self._dbusservice.add_path('/Position', 0) # DSTK_2022-10-25 bewirkt nichts ???
    self._dbusservice.add_path('/UpdateIndex', 0)

    for path, settings in self._paths.items():
      self._dbusservice.add_path(
        path, settings['initial'], gettextcallback=settings['textformat'], writeable=True, onchangecallback=self._handlechangedvalue)

    # now _update ios called from on_message: 
    #   gobject.timeout_add(1000, self._update) # pause 1000ms before the next request

  
  def _update(self):

    if not power_sum is None: self._dbusservice['/Ac/Power'] =  power_sum # positive: consumption, negative: feed into grid
    if not power_l1 is None: self._dbusservice['/Ac/L1/Power'] = power_l1
    if not power_l2 is None: self._dbusservice['/Ac/L2/Power'] = power_l2
    if not power_l3 is None: self._dbusservice['/Ac/L3/Power'] = power_l3

    if not energy_180 is None: self._dbusservice['/Ac/Energy/Forward'] = energy_180 # energy bought from the grid
    if not energy_280 is None: self._dbusservice['/Ac/Energy/Reverse'] = energy_280 # energy sold to the grid

    self._dbusservice['/Ac/L1/Voltage'] = 230
    self._dbusservice['/Ac/L2/Voltage'] = 230
    self._dbusservice['/Ac/L3/Voltage'] = 230

    if not power_l1 is None: self._dbusservice['/Ac/L1/Current'] = round(power_l1 / 230, 2)
    if not power_l2 is None: self._dbusservice['/Ac/L2/Current'] = round(power_l2 / 230, 2)
    if not power_l3 is None: self._dbusservice['/Ac/L3/Current'] = round(power_l3 / 230, 2)

    #if not power_sum is None: logging.debug("House Consumption: {:.0f} W".format(power_sum))
    #if not power_l1 is None: logging.debug("power_l1: {:.0f} W".format(power_l1))
    #if not power_l2 is None: logging.debug("power_l2: {:.0f} W".format(power_l2))
    #if not power_l3 is None: logging.debug("power_l3: {:.0f} W".format(power_l3))
    #if not energy_180 is None: logging.debug("energy_180: {:.0f} kWh".format(energy_180))
    #if not energy_280 is None: logging.debug("energy_280: {:.0f} kWh".format(energy_280))

    # increment UpdateIndex - to show that new data is available
    index = self._dbusservice[path_UpdateIndex] + 1  # increment index
    if index > 255:   # maximum value of the index
      index = 0       # overflow from 255 to 0
    self._dbusservice[path_UpdateIndex] = index

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

  global dbusservice
  dbusservice = DbusDummyService(
    #servicename='com.victronenergy.grid',
    servicename='com.victronenergy.grid.cgwacs_edl21_ha',
    deviceinstance=0,
    paths={
      '/Ac/Power': {'initial': None, 'textformat': _w},
      '/Ac/L1/Voltage': {'initial': None, 'textformat': _v},
      '/Ac/L2/Voltage': {'initial': None, 'textformat': _v},
      '/Ac/L3/Voltage': {'initial': None, 'textformat': _v},
      '/Ac/L1/Current': {'initial': None, 'textformat': _a},
      '/Ac/L2/Current': {'initial': None, 'textformat': _a},
      '/Ac/L3/Current': {'initial': None, 'textformat': _a},
      '/Ac/L1/Power': {'initial': None, 'textformat': _w},
      '/Ac/L2/Power': {'initial': None, 'textformat': _w},
      '/Ac/L3/Power': {'initial': None, 'textformat': _w},
      '/Ac/Energy/Forward': {'initial': None, 'textformat': _kwh}, # energy bought from the grid
      '/Ac/Energy/Reverse': {'initial': None, 'textformat': _kwh}, # energy sold to the grid
    })

  logging.info('Connected to dbus, and switching over to gobject.MainLoop() (= event based)')
  mainloop = gobject.MainLoop()
  mainloop.run()

# Konfiguration MQTT
client = mqtt.Client(MQTTNAME) # create new instance
if (MQTT_USERNAME != "") and (MQTT_PASSWORD != ""):
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.on_disconnect = on_disconnect
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER_ADDRESS)  # connect to broker

client.loop_start()

if __name__ == "__main__":
  main()