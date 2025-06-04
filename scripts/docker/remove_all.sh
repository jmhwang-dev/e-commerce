#!/bin/bash

# Stop and remove all containers and images.
docker stop $(docker ps -aq)
docker rm $(docker ps -aq)
docker rmi -f $(docker images -aq)
docker network prune -f
docker volume prune -f

