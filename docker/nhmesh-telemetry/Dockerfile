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

# Command to run the application
CMD python /app/nhmesh-telemetry/producer.py
