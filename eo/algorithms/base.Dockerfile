# base.Dockerfile
FROM python:3.12-slim
WORKDIR /app

# 1. Install uv for fast package installation
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 2. Install common meta-dependencies required by the wrapper and other utils
COPY ./requirements.txt meta_requirements.txt
RUN uv pip install --system -r meta_requirements.txt

# 3. Install common system-level dependencies (like GDAL)
# Make newest libraries available, debian is by default rather outdated aka "stable"
COPY util/unstable.sources /etc/apt/sources.list.d/unstable.sources
RUN apt-get update && \
    apt-get install -y binutils libproj-dev gdal-bin g++ && \
    apt-get -y --no-upgrade -t unstable install libgdal-dev && \
    rm -rf /var/lib/apt/lists/*

# 4. Copy common wrapper scripts
COPY ./cwl_wrapper.py ./cwl_wrapper.py
COPY ./prefect_wrapper.py ./prefect_wrapper.py
COPY ./__init__.py .
