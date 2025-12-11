docker buildx create --name multi-builder --driver docker-container --use
docker run --privileged --rm tonistiigi/binfmt --install all
bash scripts/utils/build_image/warehouse.sh


export CR_PAT=
echo $CR_PAT | docker login ghcr.io -u jmhwang-dev --password-stdin

# push spark image for warehouse
docker tag warehouse:sp3.5.6-ice1.9.1 ghcr.io/jmhwang-dev/warehouse:sp3.5.6-ice1.9.1
docker push ghcr.io/jmhwang-dev/warehouse:sp3.5.6-ice1.9.1