# Use an official Python runtime as a parent image
FROM python:3.13-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies required by Playwright
RUN apt-get update && apt-get install -y \
    libglib2.0-0 \
    libnss3 \
    libnss3-tools \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libgio-cil \
    libexpat1 \
    libatspi2.0-0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libxcb1 \
    libxkbcommon0 \
    libasound2 \
    postgresql-client \
    --no-install-recommends && rm -rf /var/lib/apt/lists/*

# Copy the current directory contents into the container at /app
COPY . /app

COPY wait-for-db.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/wait-for-db.sh

# Install any needed packages specified in requirements.txt
ENV PIP_DEFAULT_TIMEOUT=1000
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN playwright install

# Expose the port the app runs on (if any) - not strictly needed for a script
# EXPOSE 80

# Run the command to start the script
# This will be overridden by docker-compose
CMD ["python", "speedway_scraper/spiders/ekstraliga_match.py"]
