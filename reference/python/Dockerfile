FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY nvlink_rack_scheduler ./nvlink_rack_scheduler
RUN pip install --no-cache-dir .
USER 65534
ENTRYPOINT ["nvlink-rack-scheduler"]
