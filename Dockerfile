# Multi-stage optimized builder for OpenCV retail edge node pipelines
FROM python:3.11-slim as builder

WORKDIR /app

# Install system compilation packages for OpenCV and FFmpeg
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgl1 \
    libglib2.0-0 \
    libgomp1 \
    ffmpeg \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements or setup direct production pip installations
COPY requirements.txt .

# Install PyTorch CPU-only version first to avoid downloading heavy CUDA dependencies (saves ~2.5GB of download)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir torch torchvision --index-url https://download.pytorch.org/whl/cpu && \
    (pip install --no-cache-dir -r requirements.txt || \
     pip install --no-cache-dir fastapi uvicorn sqlalchemy pydantic opencv-python-headless numpy)

# Production runtime container stage
FROM python:3.11-slim

WORKDIR /app

# Install runtime graphics support libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Copy built python modules from builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy app codebase
COPY . .

# Expose FastAPI REST and WebSocket ports
EXPOSE 8000

ENV ENV=production
ENV DEBUG=false

# Command executes the FastAPI backend which auto-boots CV threads
CMD ["python", "-m", "backend.main"]
