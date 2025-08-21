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