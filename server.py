
#!/usr/bin/python
import time
import serial
import logging
import requests
import json
import sys
import os
from paho.mqtt import client as mqtt_client
from daemon import Daemon

MQTT_API_URL = os.environ.get('MQTT_API_URL', None)
MQTT_CLIENT_ID = os.environ.get('MQTT_CLIENT_ID', 'DDS238d')
MQTT_SERVER = os.environ.get('MQTT_SERVER', '127.0.0.1')
MQTT_PORT = os.environ.get('MQTT_PORT', 1883)
MQTT_USER = os.environ.get('MQTT_USER', 'dds238')
MQTT_PASS = os.environ.get('MQTT_PASS', 'password')
DDS238D_USB_PORT = os.environ.get('DDS238D_USB_PORT', '/dev/ttyUSB0')
DDS238D_PIDFILE = os.environ.get('DDS238D_PIDFILE', '/var/run/DDS238d.pid')
DDS238D_LOGFILE = os.environ.get('DDS238D_LOGFILE', '/var/log/DDS238d.log')

FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = 12
MAX_RECONNECT_DELAY = 60

# Configure logging
logging.basicConfig(filename=DDS238D_LOGFILE,level=logging.DEBUG)

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT Broker!")
        else:
            logging.error("Failed to connect, return code %d\n", rc)

    def on_disconnect(client, userdata, rc):
        logging.info("Disconnected with result code: %s", rc)
        reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
        while reconnect_count < MAX_RECONNECT_COUNT:
            logging.info("Reconnecting in %d seconds...", reconnect_delay)
            time.sleep(reconnect_delay)

            try:
                client.reconnect()
                logging.info("Reconnected successfully!")
                return
            except Exception as err:
                logging.error("%s. Reconnect failed. Retrying...", err)

            reconnect_delay *= RECONNECT_RATE
            reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
            reconnect_count += 1
        logging.info("Reconnect failed after %s attempts. Exiting...", reconnect_count)

    if MQTT_API_URL is not None:
        # Don't connect if we work via EMQX API interface
        return None

    # Set Connecting Client ID
    client = mqtt_client.Client(MQTT_CLIENT_ID)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.connect(MQTT_SERVER, port=MQTT_PORT, keepalive=15)
    return client


class App(Daemon):
    def __init__(self):
        self.pidfile_timeout = 5

        self.bytes_read = []
        self.consumed = 0.0
        self.voltage = 0.0
        self.current = 0.0
        self.power = 0.0
        self.frequency = 0.0
        self.power_factor = 0.0
       
        self.mqtt = connect_mqtt()
        #TODO: self.mqtt.on_disconnect = on_disconnect
        self.mqtt.loop_start()

    def __del__(self):
        self.mqtt.loop_stop()
        self.ser.close()

    def word(self, pos):
        return self.data[pos+3]*256 + self.data[pos+4]

    def double_word(self, pos):
        return self.word(pos) * 256 * 256 + self.word(pos + 2)

    def update_mqtt(self, t, v):
        if MQTT_API_URL is not None:
            d = {
                "topic": "DDS238/0/{}".format(t),
                "payload": "{}".format(v)
            }
            r = requests.post(MQTT_API_URL, json=d, auth=(MQTT_USER, MQTT_PASS))
            if not r.ok:
                logging.debug(json.dumps(r))
                logging.error("Error updating MQTT API value")
        else:
            r = self.mqtt.publish("DDS238/0/{}".format(t), "{}".format(v))
            if not r.is_published:
                logging.debug(json.dumps(r))
                logging.error("Error updating MQTT value")

    def run(self):
        while True:
            ser = serial.Serial(port=DDS238D_USB_PORT, baudrate=9600, bytesize=serial.EIGHTBITS, timeout=0.5, parity=serial.PARITY_NONE, stopbits=serial.STOPBITS_ONE, xonxoff=False, rtscts=False, dsrdtr=False)
            self.bytes_read = ser.write(b'\x01\x03\x00\x08\x00\x0a\x44\x0f')
            self.data = ser.read(25)
            ser.close()

            consumed = frequency = voltage = current = power = pf = 0.0

            if len(self.data) == 25:
                consumed = self.double_word(4)/100.0
                frequency = self.word(18)/100.0
                voltage = self.word(8)/10.0
                current = self.word(10)/100.0
                power = self.word(12)
                pf = self.word(16)/1000.0

            if self.consumed != consumed:
                self.consumed = consumed
                self.update_mqtt('consumed', consumed)

            if self.frequency != frequency:
                self.frequency = frequency
                self.update_mqtt('frequency', frequency)

            if self.voltage != voltage:
                self.voltage = voltage
                self.update_mqtt('voltage', voltage)

            if self.current != current:
                self.current = current
                self.update_mqtt('current', current)

            if self.power != power:
                self.power = power
                self.update_mqtt('power', power)

            if self.power_factor != pf:
                self.power_factor = pf
                self.update_mqtt('powerFactor', pf)

            time.sleep(0.2)

if __name__ == "__main__":
    daemon = App(DDS238D_PIDFILE)
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print("Unknown command")
            sys.exit(2)
        sys.exit(0)
    else:
        print("usage: %s start|stop|restart" % sys.argv[0])
        sys.exit(2)
