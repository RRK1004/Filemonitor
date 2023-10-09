import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import paho.mqtt.client as mqtt
import argparse
import os


class MyHandler(FileSystemEventHandler):
    def __init__(self, client,path):
        super().__init__()
        self.client = client
        self.path = path
    
    def on_modified(self, event):
        if event.is_directory:
            return
        print(f"File {event.src_path} has been modified.")
        # Publish a message to MQTT
        self.client.publish("filesystem", f"File {event.src_path} has been modified.")
        if os.path.isdir(self.path):
            res=self.get_folder_size()
            print(f"Folder modified {event.src_path} has been modified {res[1]}.")

    def on_deleted(self, event):
        if event.is_directory:
            return
        print(f"File {event.src_path} has been deleted.")
        # Publish a message to MQTT
        self.client.publish("filesystem", f"File {event.src_path} has been deleted.")
        if os.path.isdir(self.path):
            res = self.get_folder_size()
            print(f"Folder {event.src_path} has been modified {res[1]}.")
    
    def get_folder_size(self):
        total_size = 0

        for dirpath, dirnames, filenames in os.walk(self.path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                total_size += os.path.getsize(file_path)

        # Convert total_size to a human-readable format
        if total_size < 1024:
            size_str = f"{total_size} bytes"
        elif total_size < 1024 * 1024:
            size_str = f"{total_size / 1024:.2f} KB"
        elif total_size < 1024 * 1024 * 1024:
            size_str = f"{total_size / (1024 * 1024):.2f} MB"
        else:
            size_str = f"{total_size / (1024 * 1024 * 1024):.2f} GB"

        return total_size, size_str
   

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

      # Check Directory Permissions
    if os.path.isdir(file_path):
        for dirpath, dirnames, filenames in os.walk(file_path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                if os.access(file_path, os.R_OK):
                    print(f"permission access '{file_path}'")
                else:
                    print(f"Permission denied for directory '{file_path}'. Exiting.")
                    exit()

    # Set up MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(address, int(port), 60)  # Update with your MQTT broker information
    # Set up filesystem monitor with the client

    path = file_path  # Update with the directory you want to monitor
    event_handler = MyHandler(client,path)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path")
    parser.add_argument("--port")
    parser.add_argument("--address")
    args = parser.parse_args()
    main(args.path, args.port, args.address)
