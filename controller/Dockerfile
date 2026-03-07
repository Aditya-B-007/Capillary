# Stage 1: Builder
FROM python:3.11-slim as builder
WORKDIR /app
COPY requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends gcc python3-dev && \
    pip install --user --no-cache-dir -r requirements.txt fastapi uvicorn websockets && \
    rm -rf /var/lib/apt/lists/*

# Stage 2: Final Lean Image
FROM python:3.11-slim
WORKDIR /app
RUN useradd -m -u 1000 appuser && chown appuser:appuser /app

COPY --from=builder /root/.local /home/appuser/.local

COPY --chown=appuser:appuser common/ ./common/
COPY --chown=appuser:appuser controller/ ./controller/
ENV PYTHONPATH=/app \
    PATH=/home/appuser/.local/bin:$PATH
USER appuser
CMD ["python", "-m", "controller.main"]