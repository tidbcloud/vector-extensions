#!/bin/bash
# AWS 凭证配置脚本示例

echo "配置 AWS S3 访问凭证"
echo "===================="
echo ""

# 方式 1: 通过环境变量（推荐用于测试）
echo "方式 1: 环境变量配置"
echo "export AWS_ACCESS_KEY_ID=\"your-access-key-id\""
echo "export AWS_SECRET_ACCESS_KEY=\"your-secret-access-key\""
echo "export AWS_REGION=\"us-west-2\""
echo ""

# 方式 2: 通过 AWS credentials 文件
echo "方式 2: AWS Credentials 文件 (~/.aws/credentials)"
echo "创建文件: mkdir -p ~/.aws && cat > ~/.aws/credentials <<EOF"
echo "[default]"
echo "aws_access_key_id = your-access-key-id"
echo "aws_secret_access_key = your-secret-access-key"
echo "region = us-west-2"
echo "EOF"
echo ""

# 验证配置
echo "验证配置:"
echo "  - 测试 S3 访问: aws s3 ls s3://your-bucket-name/"
echo "  - 或使用 Python: python3 -c \"import boto3; print(boto3.client('s3').list_buckets())\""
echo ""

# 设置其他环境变量
echo "其他可选环境变量:"
echo "export VECTOR_BINARY=\"vector\"  # Vector 二进制路径"
echo "export CONFIG_DIR=\"/tmp/vector-tasks\"  # 配置文件目录"
