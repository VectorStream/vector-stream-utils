# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the application code into the container
COPY kafka_producer.py app.py requirements.txt /app/

# Install required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port that Flask will run on
EXPOSE 5000

# Define environment variable for verbose logging
ENV PYTHONUNBUFFERED=1

# Ensure the container runs as a non-root user
RUN useradd -m user && chown -R user /app
USER user

# Command to run the Flask application
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]
