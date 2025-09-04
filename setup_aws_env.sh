#!/bin/bash

# 这个脚本演示如何设置基础 AWS 凭证来让 AssumeRole 工作
# 你需要替换下面的占位符为真实的凭证

echo "设置基础 AWS 凭证（需要替换为真实凭证）:"
echo ""

# 方式1: 使用环境变量（推荐）
echo "export AWS_ACCESS_KEY_ID=AKIA..." # 替换为你的基础访问密钥
echo "export AWS_SECRET_ACCESS_KEY=..."  # 替换为你的基础秘密密钥  
echo "export AWS_DEFAULT_REGION=us-west-2"
echo ""

# 或者方式2: 使用现有的 AWS Profile
echo "# 或者使用现有的 AWS Profile:"
echo "export AWS_PROFILE=default"  # 或者你的 profile 名称
echo ""

echo "设置凭证后，运行测试:"
echo "timeout 30s ./target/release/vector --config test_system_tables_simple.yaml"
echo ""

echo "如果你已经有了基础凭证，可以直接测试:"
echo "./test_tidb_systemtable.sh --bucket csn-dev-test --region us-west-2 --assume-role arn:aws:iam::385595570414:role/dbaas-dev --duration 30"
