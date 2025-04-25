# Use a Python base image.  You can choose a specific version or use a tag like "latest"
FROM python:3.13

# Set the working directory inside the container
WORKDIR /app

# Install the Python dependencies
RUN pip install --no-cache-dir poetry

# Copy the application code into the container
COPY . /app

RUN poetry config virtualenvs.create false \
    && poetry install --no-root

# Expose the MQTT port (if needed)
EXPOSE 1883

# Set environment variables.  These can be overridden when running the container.
ENV BROKER='mqtt.nhmesh.live'
ENV PORT=1883
ENV TOPIC='/msh//2/json/'
ENV USERNAME=''
ENV PASSWORD=''
ENV NODE_IP=''

# Command to run the application
CMD python /app/nhmesh-telemetry/producer.py --broker "$BROKER" --port $PORT --topic "$TOPIC" --username "$USERNAME" --password "$PASSWORD" --node-ip "$NODE_IP"