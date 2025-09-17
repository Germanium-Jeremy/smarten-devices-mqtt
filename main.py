import json
import random
import time
import paho.mqtt.client as mqtt

# Unique Identifiers for sensors
UNIQUE_IDENTIFIERS = [
     "AnotherSensor1",
     "AnotherSensor2",
     "AnotherSensor3",
     "AnotherSensor4",
     "AnotherSensor5",
     "AnotherSensor6",
     "AnotherSensor7",
     "AnotherSensor8",
     "AnotherSensor9",
     "AnotherSensor10"
]

# MQTT Configuration
MQTT_BROKER = "157.173.101.159"  # Replace with your MQTT broker address
# MQTT_BROKER = "localhost"  # Replace with your MQTT broker address
MQTT_PORT = 1883

# Global variables to hold sensor states
sensors = {identifier: {"accumulated_volume": 0.0, "status": "OFF"} for identifier in UNIQUE_IDENTIFIERS}

# MQTT Client setup
client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
     print("Connected to MQTT Broker!")
     for identifier in UNIQUE_IDENTIFIERS:
          command_topic = f"{identifier}/sensor/command"
          client.subscribe(command_topic)

def on_message(client, userdata, message):
     global sensors
     payload = message.payload.decode()
     command_data = json.loads(payload)  # Parse the incoming JSON command
     identifier = message.topic.split('/')[0]  # Extract identifier from topic
     print(f"Received command for {identifier}: {payload}\n")

     if identifier in sensors:
          command = command_data.get("command", "").upper()  # Get command and convert to uppercase
          if command == "OFF":
               sensors[identifier]["status"] = "OFF"
               print(f"{identifier} status updated to OFF. Stopping data transmission.\n")
          elif command == "ON":
               sensors[identifier]["status"] = "ON"
               print(f"{identifier} status updated to ON. Resuming data transmission.\n")

def generate_flow_rate():
     return random.uniform(0, 25)  # Random flow rate between 0 and 25

def calculate_volume(flow_rate, identifier):
     global sensors
     volume = (flow_rate * 3) / 60.0
     sensors[identifier]["accumulated_volume"] += volume
     return sensors[identifier]["accumulated_volume"]

def main():
     client.on_connect = on_connect
     client.on_message = on_message
     client.connect(MQTT_BROKER, MQTT_PORT, 60)

     client.loop_start()  # Start the MQTT loop

     try:
          while True:
               for identifier in UNIQUE_IDENTIFIERS:
                    if sensors[identifier]["status"] == "ON":
                         flow_rate = generate_flow_rate()
                         volume = calculate_volume(flow_rate, identifier)
                         data = {
                              "flowRate": round(flow_rate, 2),
                              "volume": round(volume, 2),
                              "status": sensors[identifier]["status"]
                         }
                         mqtt_topic = f"{identifier}/sensor/data"
                         client.publish(mqtt_topic, json.dumps(data))
                         print(f"Published data for {identifier}: {data}")
                    else:
                         print(f"Data transmission for {identifier} is OFF.")
               
               print()

               time.sleep(15)  # Wait for 15 seconds
     except KeyboardInterrupt:
          print("Exiting...")
     finally:
          client.loop_stop()
          client.disconnect()

if __name__ == "__main__":
     main()
