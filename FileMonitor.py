import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import paho.mqtt.client as mqtt
import argparse
import os

class MyHandler(FileSystemEventHandler):
    def __init__(self, client,file_chunk_size=1024):
        super().__init__()
        self.client = client
        self.file_chunk_size = file_chunk_size


    def on_modified(self, event):
        if event.is_directory:
            return
        print(f'File {event.src_path} has been modified.')
    
    ## Publish a message to MQTT
        self.publish_large_file(event.src_path)

    def publish_large_file(self, file_path):
        try:
            with open(file_path, 'rb') as file:
                chunk = file.read(self.file_chunk_size)
                while chunk:
                    self.client.publish("filesystem", chunk)
                    chunk = file.read(self.file_chunk_size)
        except FileNotFoundError:
            print(f"File '{file_path}' not found.")
        except Exception as e:
            print(f"Error publishing large file: {e}")

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("filesystem")


# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         print(f"Connected to MQTT broker.")
#         client.subscribe("filesystem")
#     else:
#         print(f"Connection to MQTT broker failed with result code {rc}.")
#         attempt_reconnect(client)

# def attempt_reconnect(client):
#     max_reconnect_attempts = 5
#     current_attempt = 0

#     while not client.is_connected() and current_attempt < max_reconnect_attempts:
#         try:
#             print("Attempting to reconnect to MQTT broker...")
#             client.reconnect()
#             time.sleep(2 ** current_attempt)  # Wait for a short time before checking the connection status
#             current_attempt += 1
#         except Exception as e:
#             print(f"Reconnection attempt failed: {e}")


#     if client.is_connected():
#         print("Reconnected to MQTT broker.")       


def on_message(client, userdata, msg):
    print(f"Received message: {msg.payload}")

def main(file_path, port, address,chunk_size):

    # Check if Path Exists
    if not os.path.exists(file_path):
        print(f"The specified path '{file_path}' does not exist. Exiting.")
        exit()

    # Check if Path is a Directory
    if not os.path.isdir(file_path):
        print(f"The specified path '{file_path}' is not a directory. Exiting.")
        exit()

    # Check Directory Permissions
    if not os.access(file_path, os.R_OK):
        print(f"Permission denied for directory '{file_path}'. Exiting.")
        exit()

    # Set up MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(address, int(port), 60)  # Update with your MQTT broker information

    # Set up filesystem monitor with the client
    path = file_path  # Update with the directory you want to monitor
    event_handler = MyHandler(client,chunk_size)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    # Script cleans up resources and exits

    try:
        while True:
            time.sleep(1)
            # if not client.is_connected():
            #     attempt_reconnect(client)
    except KeyboardInterrupt:
        print("Script interrupted. Exiting.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        observer.stop()
        observer.join()
        client.disconnect()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--path")
    parser.add_argument("--port")
    parser.add_argument("--address")
    parser.add_argument("--chunk-size", type=int, default=1024)
    args = parser.parse_args()
    main(args.path, args.port, args.address, args.chunk_size)

