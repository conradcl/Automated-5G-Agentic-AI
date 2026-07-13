#!/usr/bin/env bash

cd "$(dirname "$0")/.."
source .venv/bin/activate
PYTHONPATH=src python -m oran_agent.chat