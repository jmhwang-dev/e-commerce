ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then \
        curl -L -o /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-arm64/mc; \
    else \
        curl -L -o /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc; \
    fi && \
    chmod +x /usr/local/bin/mc