docker buildx create --name multi-builder --driver docker-container --use
docker run --privileged --rm tonistiigi/binfmt --install all
bash scripts/utils/build_image/warehouse.sh