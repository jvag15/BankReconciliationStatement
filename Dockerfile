# BRS Engine — multi-stage build
# Stage 1 builds the React SPA, stage 2 assembles a Python image with the
# dist baked into /app/static so brs_web serves both API + SPA. brs_worker
# builds from this same Dockerfile (see docker-compose.yml); the frontend
# stage layer-caches so it doesn't cost the worker a second build.

# ─── Stage 1: SPA build ─────────────────────────────────────────────────
FROM node:20-alpine AS frontend-builder
WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install --silent

COPY index.html vite.config.ts tsconfig.json tsconfig.node.json tailwind.config.js postcss.config.js ./
COPY src ./src

ARG VITE_API_AUTH_TOKEN=dev-token
ENV VITE_API_AUTH_TOKEN=$VITE_API_AUTH_TOKEN
RUN npm run build


# ─── Stage 2: Python runtime ────────────────────────────────────────────
FROM python:3.11-slim AS backend
WORKDIR /app

# Install system dependencies required by scipy/LightGBM/psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgomp1 \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --default-timeout=1000 --retries 10 --no-cache-dir -r requirements.txt

COPY app ./app
COPY alembic ./alembic
COPY alembic.ini .
COPY pyproject.toml .
COPY tests ./tests
COPY scripts ./scripts
COPY --from=frontend-builder /app/dist ./static

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
