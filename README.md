# E-Commerce Data Platform

This repository contains work for building a Kubernetes based data platform and doing exploratory analysis with the Olist Brazilian e-commerce dataset.

- `infra/` - Infrastructure as code for local clusters
  - `k8s/` - Kubernetes manifests and deployment scripts for MinIO and Spark
  - `ansible/` - Node provisioning playbooks
  - `hadoop/` - Hadoop configuration for HDFS
- `scripts/` - Utility scripts for local development
  - `data/` - Dataset download helper
- `src/` - Application source code such as inference pipelines
- `notebooks/` - Jupyter notebooks for EDA
- `docs/` - Project documentation

## Quick Start

1. Download the datasets (requires the Kaggle CLI configured with your credentials):
   ```bash
   bash scripts/data/download_olist.sh
   ```
2. Copy environment variables and deploy MinIO and Spark locally:
   ```bash
   cp .env.example infra/k8s/.env
   cd infra/k8s/scripts
   ./deploy_all.sh
   ```
   After deployment the MinIO console is reachable at `http://localhost:30901` using the credentials from your `.env` file.

Additional handy kubectl and helm commands can be found in [docs/commands.md](docs/commands.md).

