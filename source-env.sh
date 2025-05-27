#!/bin/bash

# Load .env and export variables
set -a                # Automatically export all variables
source .env           # Source the file
set +a                # Stop auto-exporting