#!/bin/bash

# 모든 컨테이너와 이미지를 중지 후 삭제합니다.
docker stop $(docker ps -aq)
docker rm $(docker ps -aq)
docker rmi -f $(docker images -aq)
docker network prune -f
docker volume prune -f

