#!/bin/bash
# UHC Pipeline Setup Script for EC2
# Paste this entire script into the EC2 terminal and run it

set -e  # Exit on any error

echo "================================"
echo "UHC Pipeline EC2 Setup"
echo "================================"

# Step 1: Update system
echo "[1/5] Updating system..."
sudo apt update && sudo apt upgrade -y > /dev/null 2>&1
echo "✓ System updated"

# Step 2: Install dependencies
echo "[2/5] Installing Python and Git..."
sudo apt install -y python3-pip python3-venv git > /dev/null 2>&1
echo "✓ Python and Git installed"

# Step 3: Clone repo
echo "[3/5] Cloning repository..."
cd /home/ubuntu
git clone https://github.com/jntv/asessment-repo.git
cd asessment-repo
echo "✓ Repository cloned"

# Step 4: Install Python packages
echo "[4/5] Installing Python dependencies..."
pip3 install -r requirements.txt > /dev/null 2>&1
echo "✓ Dependencies installed"

# Step 5: Copy .env template
echo "[5/5] Setting up .env template..."
cp .env.example .env
echo "⚠️  IMPORTANT: Edit .env with your actual credentials:"
echo "    - Snowflake credentials (SF_USER, SF_PASSWORD, SF_ACCOUNT, etc.)"
echo "    - AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)"
echo "    - S3 bucket and prefix"
echo "    - Snowflake S3 role ARN"
echo ""
echo "Then run: python3 src/pipeline.py"

echo ""
echo "================================"
echo "Setup Complete!"
echo "================================"
echo ""
echo "Ready to run pipeline:"
echo "  python3 src/pipeline.py"
echo ""
echo "This will:"
echo "  - Download 359 files from UHC"
echo "  - Parse them to Parquet"
echo "  - Upload to S3: uhc-parquet-data/parquet/"
echo "  - Load into Snowflake"
echo ""
echo "Estimated time: 2-4 hours"
echo ""
