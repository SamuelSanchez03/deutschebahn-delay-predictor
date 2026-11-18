# API del predictor de retrasos (FastAPI + LightGBM)
FROM python:3.12-slim

# LightGBM necesita la librería de OpenMP en tiempo de ejecución
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/
COPY model/ model/

# Usuario sin privilegios
RUN useradd --create-home api && chown -R api /srv
USER api

# Render define la variable PORT; localmente se usa 8000
ENV PORT=8000
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import urllib.request,os;urllib.request.urlopen(f'http://127.0.0.1:{os.environ[\"PORT\"]}/health')"
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
