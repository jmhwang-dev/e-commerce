docker build --no-cache \
  -t superset:thrift-0.7.0-psql16 \
  ./infra/superset \
&& docker image prune -f