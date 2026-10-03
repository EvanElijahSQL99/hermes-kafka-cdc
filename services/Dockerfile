# One image for all four Python services; the API stage also serves the built dashboard.
# Build context is the repository root (see docker-compose.yml).

FROM node:22-slim AS ui
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY ui/ ./
RUN npm run build

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 UI_DIR=/app/ui
WORKDIR /app
COPY services/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/hermes ./hermes
COPY --from=ui /ui/dist ./ui
RUN useradd --system --uid 10001 hermes
USER hermes
