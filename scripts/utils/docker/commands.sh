# compose 실행 후 삭제
docker compose up --abort-on-container-exit && docker compose down --volumes --remove-orphans

# 실패 로그 출력
docker logs <container-name>
