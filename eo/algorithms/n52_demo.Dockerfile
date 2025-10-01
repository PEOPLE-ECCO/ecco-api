FROM python:3.12-slim
WORKDIR app
ARG ALGORITHM_BASE

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Meta-Dependencies needed for job handling in docker
COPY ./requirements.txt meta_requirements.txt
RUN uv pip install --system -r meta_requirements.txt

# Install actual app depenencies
COPY ./n52_demo/requirements.txt requirements.txt
RUN uv pip install --system -r requirements.txt

ENV ALGORITHM_BASE=${ALGORITHM_BASE}

# Copy everything over
COPY ./n52_demo/ ./n52_demo/
COPY __init__.py .
COPY wrapper.py .
