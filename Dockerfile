# Use official Python image
FROM python:3.11-slim

WORKDIR /

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

# Do not copy games/ and live_feeds/ into the container
# These directories should be mounted as volumes at runtime
# Example run command:
# docker run -v /path/to/games:/app/games -v /path/to/live_feeds:/app/live_feeds mlbet

# Default command (can be overridden)
CMD ["python", "pull_games.py"]
