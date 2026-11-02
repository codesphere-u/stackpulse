FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY pyproject.toml README.md ./
COPY stackpulse ./stackpulse

RUN pip install --no-cache-dir .

EXPOSE 8098

CMD ["stackpulse", "serve", "--host", "0.0.0.0", "--port", "8098"]
