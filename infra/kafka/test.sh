curl -X POST http://localhost:8000/publish \
  -H "Content-Type: application/json" \
  -d '{"key": "user-123", "value": {"action": "purchase", "amount": 99.99}}'
