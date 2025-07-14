docker compose -f infra/docker-compose.yml exec -it dbt \
bash -c "cd my_dbt && dbt run -s gold.is_late"
