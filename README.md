# e-commerce

## setup
```bash
# install dependencies
curl -sSL https://install.python-poetry.org | python3 -
poetry install

# download dataset
bash ./scripts/etc/download_olist.sh
cd infrastructure/local/minio
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
