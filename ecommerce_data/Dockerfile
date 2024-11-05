# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the application code into the container
COPY kafka_producer.py app.py /app/

# Install required Python packages
RUN pip install --no-cache-dir asyncua aiokafka flask

# Expose the port that Solara will run on
EXPOSE 8765

# Define environment variable for verbose logging
ENV PYTHONUNBUFFERED=1

# Ensure the container runs as a non-root user
RUN useradd -m user && chown -R user /app
USER user

# Run the Solara application using the Solara CLI
CMD ["solara", "run", "solara_app.py", "--host=0.0.0.0", "--port=8765"]
