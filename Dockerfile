FROM python:3.13-alpine AS base

LABEL maintainer="Jan Speckamp <j.speckamp@52north.org>" \
      org.opencontainers.image.authors="Jan Speckamp <j.speckamp@52north.org>" \
      org.opencontainers.image.url="https://github.com/PEOPLE-ECCO/ecco-api" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.vendor="52°North GmbH" \
      org.opencontainers.image.licenses="Apache 2.0" \
      org.opencontainers.image.ref.name="PEOPLE-ECCO/ecco-api" \
      org.opencontainers.image.title="" \
      org.opencontainers.image.description=""

RUN apk add gdal gdal-dev gcc g++ musl-dev libffi-dev py3-maturin

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .


ENV PYTHONUNBUFFERED=1
FROM base AS api
CMD ["hypercorn", "-c", "hypercorn.conf.py", "app:APP"]