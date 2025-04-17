#!/bin/bash
OS_TYPE=$(uname)

if [[ "$OS_TYPE" == "Darwin" ]]; then
    brew update && brew install openjdk@8
elif [[ "$OS_TYPE" == "Linux" ]]; then
    sudo apt update && sudo apt install -y openjdk-8-jdk
else
  echo "Unsupported operating system: $OS_TYPE"
  exit 1
fi