FROM python:3.12-slim
WORKDIR app
ARG ALGORITHM_BASE

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Meta-Dependencies needed for job handling in docker
COPY ./requirements.txt meta_requirements.txt
RUN uv pip install --system -r meta_requirements.txt

# Make newest libraries available, debian is by default rather outdated aka "stable"
COPY dhi_prototype/unstable.sources /etc/apt/sources.list.d/unstable.sources
RUN apt update
RUN apt install -y binutils libproj-dev gdal-bin g++
RUN apt -y --no-upgrade -t unstable install libgdal-dev

# Install actual app depenencies
COPY ./dhi_prototype/requirements.txt requirements.txt
RUN uv pip install --system -r requirements.txt

ENV ALGORITHM_BASE=${ALGORITHM_BASE}

# Copy everything over
COPY ./dhi_prototype/ ./dhi_prototype/
COPY __init__.py .
COPY wrapper.py .