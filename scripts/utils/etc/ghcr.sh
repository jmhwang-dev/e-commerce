docker buildx create --name multi-builder --driver docker-container --use
docker run --privileged --rm tonistiigi/binfmt --install all
bash scripts/utils/build_image/warehouse.sh

# export CR_PAT=사용자_토큰_값
# echo $CR_PAT | docker login ghcr.io -u 깃허브_아이디 --password-stdin
# docker tag my-app:latest ghcr.io/user123/my-app:v1.0
# docker push ghcr.io/user123/my-app:v1.0