FROM python:3.12-slim

WORKDIR /usr/app/dbt

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir dbt-core==1.10.15 dbt-postgres==1.10.0

ENTRYPOINT ["dbt"]
CMD ["debug"]
