# Filesystem Change Monitor with MQTT Publishing

**Overview**

This project is a Filesystem Change Monitor that utilizes MQTT (Message Queuing Telemetry Transport) for publishing file change events. The monitor detects modifications, additions, and deletions in specified directories and publishes these events to an MQTT broker. Filesystem change monitor that uses the "watchdog" library to track modifications in a specified directory. Additionally, it integrates with an MQTT broker using the "paho.mqtt.client" library to publish notifications about file modifications.


**Feaures :**
- Monitor specified directories for filesystem changes.
- Publish change events (addition, modification, deletion) to an MQTT broker.
- Simple and lightweight implementation.

**Libraries Used :**

- Python 3.8
- Paho-MQTT library (install with `pip3 install watchdog paho-mqtt`)

**Pre-requisites :**

- Git need to installed as per steps in the [Git installation page](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
- Docker Need to installed as per steps in the [Visit Docker page](https://docs.docker.com/engine/install/ubuntu/)


**Installation :**
- Clone the repo
    'git clone "REPO LINK"'
    'cd FileMonitor' 

- Build Docker image 
    'docker image -t imagename .'

- Run Docker Container with builded image in one terminal
    'docker run imagename &'

- Exec Docker Container with command line arguments in another terminal
    'docker exec -it containerid python3 FileMonitor.py --address 127.0.0.1 --port 1883 --path /app'
  
        - `--path`, indicating which directory or file to monitor,
        - `--address`, indicating the MQTT broker address,
        - `--port`, indicating the MQTT broker port.

 **UseCase**
 
- File Modifications
- Large File sizes
- Frequent Changes
- Check if Path is a Directory
- Check Directory Permissions
- Exception Handling
- Command Line arguments










