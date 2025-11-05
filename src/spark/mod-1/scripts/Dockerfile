# Base image
FROM bitnami/spark:latest

# Set working directory
WORKDIR /app

# Copy application files from current directory
COPY pr-3-app.py /app/
COPY users.json /app/

# Install a simple dependency (optional)
RUN pip install --no-cache-dir numpy

# Keep container running
CMD ["tail", "-f", "/dev/null"]