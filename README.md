# E-Commerce Data Platform

This repository contains work for building a data platform and analysis environment around the Olist Brazilian e‑commerce dataset.

- `infra/k8s` - Kubernetes manifests and helper scripts for running MinIO and Spark.
- `ansible` - Automation scripts for provisioning local or remote nodes.
- `eda` - Exploratory data analysis notebooks.
- `inference` - Experimental inference pipelines.

## Quick Start

The `infra/k8s/scripts` folder provides shell scripts for deploying a local MinIO and Spark stack using Helm. Copy `.env.example` to `.env` and set your credentials first.

```bash
cp .env.example .env
cd infra/k8s/scripts
./deploy_all.sh
```
After deployment the MinIO console is reachable at `http://localhost:30901` using the credentials from your `.env` file.
