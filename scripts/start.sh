#!/bin/bash

sudo rm -rf data logs/*
bash scripts/utils/docker/down.sh
bash scripts/utils/docker/up.sh