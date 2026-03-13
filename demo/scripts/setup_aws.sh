#!/bin/bash
# AWS credentials configuration script example

echo "Configure AWS S3 access credentials"
echo "===================================="
echo ""

# Method 1: Via environment variables (recommended for testing)
echo "Method 1: Environment variable configuration"
echo "export AWS_ACCESS_KEY_ID=\"your-access-key-id\""
echo "export AWS_SECRET_ACCESS_KEY=\"your-secret-access-key\""
echo "export AWS_REGION=\"us-west-2\""
echo ""

# Method 2: Via AWS credentials file
echo "Method 2: AWS Credentials file (~/.aws/credentials)"
echo "Create file: mkdir -p ~/.aws && cat > ~/.aws/credentials <<EOF"
echo "[default]"
echo "aws_access_key_id = your-access-key-id"
echo "aws_secret_access_key = your-secret-access-key"
echo "region = us-west-2"
echo "EOF"
echo ""

# Verify configuration
echo "Verify configuration:"
echo "  - Test S3 access: aws s3 ls s3://your-bucket-name/"
echo "  - Or use Python: python3 -c \"import boto3; print(boto3.client('s3').list_buckets())\""
echo ""

# Set other environment variables
echo "Other optional environment variables:"
echo "export VECTOR_BINARY=\"vector\"  # Vector binary path"
echo "export CONFIG_DIR=\"/tmp/vector-tasks\"  # Configuration file directory"
