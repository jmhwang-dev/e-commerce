curl -X POST http://localhost:8000/send_review \
  -H "Content-Type: application/json" \
  -d @infra/shared/sample_messages/review_raw.json