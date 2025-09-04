#!/bin/bash

echo "🚀 使用 SSO Profile 测试 AssumeRole"
echo ""

# 检查是否有 sso profile
if [[ -f ~/.aws/config ]] && grep -q "\[profile sso\]" ~/.aws/config; then
    echo "✅ 找到 SSO profile 配置"
else
    echo "❌ 没有找到 SSO profile 配置"
    echo "请先配置 AWS SSO:"
    echo "  aws configure sso"
    exit 1
fi

echo ""
echo "📋 当前测试配置:"
echo "  基础凭证: AWS_PROFILE=sso"
echo "  目标角色: arn:aws:iam::385595570414:role/dbaas-dev"
echo "  S3 存储桶: csn-dev-test"
echo ""

echo "🔄 设置环境并运行测试..."
export AWS_PROFILE=sso

# 验证 SSO 登录状态
echo "🔍 检查 SSO 登录状态:"
if aws sts get-caller-identity >/dev/null 2>&1; then
    echo "✅ SSO 登录状态正常"
    aws sts get-caller-identity
else
    echo "❌ SSO 未登录或已过期"
    echo "请运行: aws sso login"
    exit 1
fi

echo ""
echo "🚀 运行 Vector AssumeRole 测试:"
timeout 30s ./target/release/vector --config test_system_tables_simple.yaml 2>&1 | \
grep -E "(AssumeRole.*成功|临时凭证|Successfully wrote|Failed to write|ERROR)" | head -10

echo ""
echo "✅ 测试完成"
