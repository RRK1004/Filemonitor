# Use an official Alpine Linux image with Python 3.8
FROM python:3.8-alpine

# Set the working directory to /app
WORKDIR /app

# Install necessary packages
RUN apk update && \
    apk add --no-cache \
    mosquitto \
    mosquitto-clients \
    bash \
    nano \
    coreutils \
    procps
# Copy the current directory contents into the container at /app
COPY . /app

# Install Python dependencies

RUN pip3 install watchdog paho-mqtt

# Run mosquitto in the background using nohup
CMD ["mosquitto"]
