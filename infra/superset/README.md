1. Clone repository

```bash
git clone --depth=1  https://github.com/apache/superset.git
export TAG=5.0.0
git fetch --depth=1 origin tag $TAG
git checkout $TAG
```

2. set env
```bash
## set `MAPBOX_API_KEY` in .env before copying
cp ./infra/superset/.env superset_repo_dir/docker
```

3. copy docker files
```bash
# superset_repo_dir: git clone directory
cp ./infra/superset/docker-compose-image-tag.yml ./infra/superset/Dockerfile superset_repo_dir
```

4. run
```bash
docker compose -f docker-compose-image-tag.yml up -d --force-recreate
```