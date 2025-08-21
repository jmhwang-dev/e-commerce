FROM apache/superset:5.0.0

USER root

COPY requirements.txt /app/docker/requirements.txt

RUN . /app/.venv/bin/activate && uv pip install --no-cache-dir -r /app/docker/requirements.txt \
    && rm -rf /root/.cache/pip

USER superset

CMD ["/app/docker/entrypoints/run-server.sh"]