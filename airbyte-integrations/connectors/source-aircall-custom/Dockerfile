FROM docker.io/airbyte/python-connector-base:4.0.2@sha256:9fdb1888c4264cf6fee473649ecb593f56f58e5d0096a87ee0b231777e2e3e73

WORKDIR /airbyte/integration_code

COPY pyproject.toml poetry.lock ./
RUN poetry install --only main --no-root

COPY source_aircall ./source_aircall
RUN poetry install --only main

ENV AIRBYTE_ENTRYPOINT="cd /airbyte/integration_code && poetry run source-aircall-custom"
ENTRYPOINT ["poetry", "run", "source-aircall-custom"]

LABEL io.airbyte.version=1.2.2
LABEL io.airbyte.name=etiennecalliesouihelp/airbyte-source-aircall
