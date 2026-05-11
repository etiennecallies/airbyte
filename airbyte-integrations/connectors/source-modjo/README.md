# Modjo Source

This is the repository for the Modjo source connector, written in Python.
For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.com/integrations/sources/modjo).

## Local development

### Prerequisites
**To iterate on this connector, make sure to complete this prerequisites section.**

#### Minimum Python version required `= 3.9.0`

#### Build & Activate Virtual Environment and install dependencies
From this connector directory, create a virtual environment:
```
python -m venv .venv
```

This will generate a virtualenv for this module in `.venv/`. Make sure this venv is active in your
development environment of choice. To activate it from the terminal, run:
```
source .venv/bin/activate
pip install -e .
```
If you are in an IDE, follow your IDE's instructions to activate the virtualenv.

#### Create credentials
Create a file `secrets/config.json` conforming to the `source_modjo/spec.yaml` file.
Note that any directory named `secrets` is gitignored across the entire Airbyte repo, so there is no danger of accidentally checking in sensitive information.
See `integration_tests/sample_config.json` for a sample config file.

**If you are an Airbyte core member**, copy the credentials in Lastpass under the secret name `source modjo test creds`
and place them into `secrets/config.json`.

### Locally running the connector
```
python main.py spec
python main.py check --config secrets/config.json
python main.py discover --config secrets/config.json
python main.py read --config secrets/config.json --catalog secrets/configured_catalog.json
python main.py read --config secrets/config.json --catalog secrets/configured_catalog.json --state secrets/state.json
```

### Locally running the connector docker image

#### Build
First, make sure you build the latest Docker image:
```
docker build . -t airbyte/source-modjo:dev
```

#### Run
Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-modjo:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-modjo:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-modjo:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/integration_tests:/integration_tests airbyte/source-modjo:dev read --config /secrets/config.json --catalog /integration_tests/configured_catalog.json
```

## Dependency Management
All of your dependencies should go in `setup.py`.

## Deploy on Dockerhub

We use account `etiennecalliesouihelp`, but this can be replaced by whatever account.
Get latest tag on https://hub.docker.com/r/etiennecalliesouihelp/airbyte-source-modjo/tags.
```shell
cd airbyte-integrations/connectors/source-modjo/
docker build --platform linux/amd64 . -t airbyte/source-modjo:{new-tag}
docker tag airbyte/source-modjo:{new-tag} etiennecalliesouihelp/airbyte-source-modjo:{new-tag}
docker login # if not already logged
docker push etiennecalliesouihelp/airbyte-source-modjo:{new-tag}
```

Alternatively, you can create a github action by looking at this [example](https://github.com/b4stien/airbyte/blob/bg/source-amplitude-oh/.github/workflows/build-and-push-docker.yml).
