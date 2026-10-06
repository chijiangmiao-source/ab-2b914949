FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HANDOFF_DB=/data/handoff.db \
    HOST=0.0.0.0 \
    PORT=8080

WORKDIR /app

# The service uses only the Python standard library; no pip install is needed.
COPY app ./app
COPY tests ./tests
COPY scripts ./scripts

RUN mkdir -p /data && chmod +x scripts/verify.sh scripts/smoke.py \
    && adduser --system --home /app --no-create-home appuser \
    && chown -R appuser /data

USER appuser
VOLUME ["/data"]

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=2s --retries=10 \
    CMD python -c "import json,urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=3); sys.exit(0 if r.status==200 and json.load(r)['status']=='ok' else 1)"

CMD ["python", "-m", "app"]
