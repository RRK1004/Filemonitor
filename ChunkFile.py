import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import paho.mqtt.client as mqtt
import argparse
import os

# Function to publish file content in chunks
def publish_file_chunks(client, topic, file_content, chunk_size):
    for i in range(0, len(file_content), chunk_size):
        chunk = file_content[i:i+chunk_size]
        client.publish(topic, chunk)
        time.sleep(0.1)  # Adjust the sleep duration as needed

# Generate a mock large file content
def generate_large_file_content(file_size):
    return "A" * file_size  # Replace with actual content generation logic


def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("filesystem")


def on_message(client, userdata, msg):
    print(f"Received message: {msg.payload}")

def main(file_path, port, address):

    # Check if Path Exists
    if not os.path.exists(file_path):
        print(f"The specified path '{file_path}' does not exist. Exiting.")
        exit()

    # Set up MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(address, int(port), 60)  # Update with your MQTT broker information

    # Define the topic and file size
    mqtt_topic = "filesystem"
    file_size = 10 * 1024 * 1024  # 10 MB (adjust as needed)

    # Generate large file content
    large_file_content = generate_large_file_content(file_size)


    # Set up filesystem monitor with the client
    path = file_path  # Update with the directory you want to monitor
    event_handler = publish_file_chunks(client)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


