FROM eclipse-mosquitto

# Create necessary directories
RUN mkdir -p /mosquitto/config
RUN mkdir -p /mosquitto/data
RUN mkdir -p /mosquitto/log

# Define volumes (these act as mount points in the container)
VOLUME /mosquitto/config
VOLUME /mosquitto/data
VOLUME /mosquitto/log

# Expose the MQTT port
EXPOSE 1883

# Command to run when the container starts
CMD ["mosquitto", "-c", "/mosquitto/config/mosquitto.conf"]