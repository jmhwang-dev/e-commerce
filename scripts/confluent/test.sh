
curl -X POST http://localhost:8000/produce

# 확인
# curl -s http://localhost:8082/subjects | jq .

docker compose -f infra/docker-compose.override.yml exec \
    kafka-avro-console-consumer --bootstrap-server kafka1:9092 --topic review_raw \
    --from-beginning \
    --property schema.registry.url=http://schema-registry:8081