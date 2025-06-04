# E-Commerce Data Platform

This repository contains work for building a data platform and analysis environment around the Olist Brazilian e‑commerce dataset.

- `infra/` - Infrastructure as code.
  - `k8s/` - Kubernetes manifests and deployment scripts for MinIO and Spark.
  - `ansible/` - Node provisioning playbooks.
  - `hadoop/` - Hadoop configuration for HDFS clusters.
- `scripts/` - Utility scripts for local development.
- `src/` - Application source code such as inference pipelines.
- `notebooks/` - Exploratory data analysis notebooks.
- `docs/` - Project documentation including ER diagrams.

## Quick Start

The `infra/k8s/scripts` folder provides shell scripts for deploying a local MinIO and Spark stack using Helm. Copy `.env.example` to `infra/k8s/.env` and set your credentials first.

```bash
cp .env.example infra/k8s/.env
cd infra/k8s/scripts
./deploy_all.sh
```
After deployment the MinIO console is reachable at `http://localhost:30901` using the credentials from your `.env` file.
