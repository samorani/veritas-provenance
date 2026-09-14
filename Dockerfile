# Search app image for Cloud Run. Build from the repository root, because the
# app imports match_artists.py from there:  docker build -t veritas-app .
FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /srv

COPY app/requirements.txt app/requirements.txt
RUN pip install --no-cache-dir -r app/requirements.txt

COPY match_artists.py ./
COPY app/ app/

RUN useradd --create-home appuser
USER appuser

# DATABASE_URL, SECRET_KEY, APP_PASSWORD come from the Cloud Run service
# configuration; set SESSION_COOKIE_SECURE=true there as well.
CMD exec gunicorn --chdir /srv/app --bind ":${PORT}" --workers 1 --threads 8 --timeout 0 app:app
