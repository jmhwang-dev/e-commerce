# link: https://docs.datahub.com/docs/quickstart

## Move To Production
# CAUTION
# Quickstart is not intended for a production environment.
# We recommend deploying DataHub to production using Kubernetes.
# We provide helpful Helm Charts to help you quickly get up and running.
# Check out Deploying DataHub to Kubernetes for a step-by-step walkthrough.

# prerequsite: `pyproject.toml`에서 requires-python = "^3.11"로 수정
conda create -n py311 python=3.11
conda activate py311
poetry env use python
poetry add acryl-datahub
poetry lock
poetry install
datahub docker quickstart   # username: datahub, password: datahub
datahub docker ingest-sample-data

## Stop DataHub
# datahub docker quickstart --stop

## Reset DataHub
# datahub docker nuke

## Upgrade DataHub
# datahub docker quickstart # or datahub docker quickstart --version v1.2.0

## Back up Datahub
# datahub docker quickstart --backup --backup-file <path to backup file>

## Restore DataHub
# datahub docker quickstart --restore