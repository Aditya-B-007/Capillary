FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*
RUN useradd -m -u 1000 appuser && chown appuser:appuser /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt fastapi uvicorn websockets

COPY --chown=appuser:appuser common/ ./common/
COPY --chown=appuser:appuser controller/ ./controller/

ENV PYTHONPATH=/app
USER appuser
CMD ["python", "-m", "controller.main"]