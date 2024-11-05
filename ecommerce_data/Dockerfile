# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory in the container to /app
WORKDIR /app

# Add current directory files to /app in container
ADD kafka_producer.py /app
ADD solara_app.py /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir asyncua aiokafka solara ipywidgets voila

# Make TCP port 8765 available to the world outside this container
EXPOSE 8765

# Define environment variable for verbose logging
ENV PYTHONUNBUFFERED=1

# Run solara_app.py when the container launches
CMD ["python", "-u", "solara_app.py"]
