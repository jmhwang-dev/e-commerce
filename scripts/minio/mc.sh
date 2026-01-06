docker run --rm -it \
  --network host \
  -v $HOME/.mc:/root/.mc \
  minio/mc \
  alias set localminio http://localhost:9000 minioadmin minioadmin
