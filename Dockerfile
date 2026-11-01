FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY clique_scheduler ./clique_scheduler
RUN pip install --no-cache-dir .
USER 65534
ENTRYPOINT ["clique-scheduler"]
