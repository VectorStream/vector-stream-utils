# Use an official Python runtime as a parent image for building
FROM python:3.9 AS builder

# Set the working directory in the container to /app
WORKDIR /app

# Copy current directory files to /app in container
COPY kafka_producer.py /app
COPY solara_app.py /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir asyncua aiokafka solara ipywidgets voila

# Final stage
FROM python:3.9-slim

# Set the working directory in the container to /app
WORKDIR /app

# Copy the installed packages from the builder stage
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy the application code
COPY kafka_producer.py /app
COPY solara_app.py /app

# Make TCP port 8765 available to the world outside this container
EXPOSE 8765

# Define environment variable for verbose logging
ENV PYTHONUNBUFFERED=1

# Ensure the container runs as a non-root user
RUN useradd -m user && chown -R user /app
USER user

# Run solara_app.py when the container launches
CMD ["python", "-u", "solara_app.py"]
