# TODO
- Modify db version `15` to `16`

- Modify `target` and `DEV_MODE` as shown below:

    ```yml
    x-common-build: &common-build
    context: .
    target: ${SUPERSET_BUILD_TARGET:-lean} # can use `dev` (default) or `lean`
        ...
    args:
        DEV_MODE: "false"
        ...
    ```

# RUN

```bash
# get superset
cd ..
git clone https://github.com/apache/superset

# Enter the repository you just cloned
$ cd superset

# Set the repo to the state associated with the latest official version
$ git checkout tags/5.0.0

# Fire up Superset using Docker Compose
$ docker compose -f docker-compose-image-tag.yml up

```