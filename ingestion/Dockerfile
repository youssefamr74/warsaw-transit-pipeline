FROM python:3.12-slim

# Print logs immediately instead of buffering them
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first, so this layer is cached when only code changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY poller.py .

# Run as a normal user, not root
RUN useradd --create-home poller
USER poller

CMD ["python", "poller.py"]