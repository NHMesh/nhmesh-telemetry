# Use a Python base image.  You can choose a specific version or use a tag like "latest"
FROM python:3.11-slim-bookworm

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt /app/requirements.txt

# Install the Python dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the application code into the container
COPY . /app

# Expose the MQTT port (if needed)
EXPOSE 1883

# Set environment variables.  These can be overridden when running the container.
ENV BROKER='mqtt.nhmesh.live'
ENV PORT=1883
ENV TOPIC='/msh//2/json/'
ENV TLS='false'
ENV USERNAME=''
ENV PASSWORD=''
ENV NODE_IP=''

# Command to run the application
CMD ["python", "/app/nhmesh-telemetry/producer.py", \
    "--broker", "$BROKER", \
    "--port", "$PORT", \
    "--topic", "$TOPIC", \
    "--tls", "$TLS", \
    "--username", "$USERNAME", \
    "--password", "$PASSWORD", \
    "--node-ip", "$NODE_IP"]
