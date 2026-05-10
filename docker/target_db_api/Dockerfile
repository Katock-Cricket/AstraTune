# Override via compose build-arg when Docker Hub is unreachable (e.g. IPv6 timeout).
ARG PYTHON_IMAGE=python:3.11-slim
FROM ${PYTHON_IMAGE}

# Optional PyPI mirror (e.g. China): pass PIP_INDEX_URL at build time; avoids files.pythonhosted.org timeouts.
ARG PIP_INDEX_URL=
ENV PIP_DEFAULT_TIMEOUT=300

WORKDIR /app
COPY requirements.txt .
RUN if [ -n "$PIP_INDEX_URL" ]; then \
      pip install --no-cache-dir -i "$PIP_INDEX_URL" -r requirements.txt; \
    else \
      pip install --no-cache-dir -r requirements.txt; \
    fi
COPY main.py .

EXPOSE 8010
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8010"]
