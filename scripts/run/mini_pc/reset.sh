docker compose \
    -f docker-compose.airflow.yml \
    -f docker-compose.spark-control-plane.yml \
    down -v

sudo rm -r ./data/airflow ./logs/airflow