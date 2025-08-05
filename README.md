# e-commerce

## setup
```bash
# install dependencies
curl -sSL https://install.python-poetry.org | python3 -
poetry install

# download dataset
bash ./scripts/etc/download_olist.sh
cd infra/local/minio
docker compose up -d
```

## preprocessing & inference jobs
```bash
poetry run python jobs/inference/run_reviews_preprocess.py
poetry run python jobs/inference/run_por2eng.py
poetry run python jobs/inference/run_sentiment.py
poetry run python jobs/inference/run_reviews_postprocess.py
```

## tests
```bash
poetry run pytest
```
Spark-related tests require `pyspark` to be installed. Configuration tests rely on
the `yaml` module provided by the `pyyaml` package.
