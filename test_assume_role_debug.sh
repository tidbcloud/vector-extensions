#!/bin/bash

# 测试 AssumeRole 功能的脚本，包含详细的调试信息

set -e

echo "=== AssumeRole 调试测试脚本 ==="
echo ""

# 检查当前环境
echo "📋 检查当前 AWS 环境:"
echo "AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-❌ 未设置}"
echo "AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-❌ 未设置}"
echo "AWS_SESSION_TOKEN: ${AWS_SESSION_TOKEN:-未设置}"
echo "AWS_PROFILE: ${AWS_PROFILE:-未设置}"
echo "AWS 配置文件: $(ls ~/.aws/credentials 2>/dev/null && echo "✅ 存在" || echo "❌ 不存在")"
echo ""

# 检查是否有基础凭证
has_basic_creds=false
if [[ -n "${AWS_ACCESS_KEY_ID:-}" ]]; then
    echo "✅ 检测到 AWS_ACCESS_KEY_ID 环境变量"
    has_basic_creds=true
elif [[ -n "${AWS_PROFILE:-}" ]]; then
    echo "✅ 检测到 AWS_PROFILE 环境变量"
    has_basic_creds=true
elif [[ -f ~/.aws/credentials ]]; then
    echo "✅ 检测到 AWS 凭证文件"
    has_basic_creds=true
else
    echo "❌ 没有检测到基础 AWS 凭证"
fi

echo ""
echo "🔧 当前状态:"
if $has_basic_creds; then
    echo "✅ 有基础凭证 - AssumeRole 应该可以工作"
    echo "📝 运行测试："
    echo "RUST_LOG=debug timeout 30s ./target/release/vector --config test_system_tables_simple.yaml"
    echo ""
    echo "🚀 开始运行测试..."
    
    # 运行测试并显示关键日志
    RUST_LOG=debug timeout 30s ./target/release/vector --config test_system_tables_simple.yaml 2>&1 | \
    grep -E "(AssumeRole|temporary credentials|Successfully wrote|Failed to write|✓|ERROR)" | \
    head -20 || echo "测试完成"
    
else
    echo "❌ 缺少基础凭证 - AssumeRole 会失败"
    echo ""
    echo "💡 解决方案："
    echo "方式1: 设置环境变量"
    echo "  export AWS_ACCESS_KEY_ID=AKIA..."
    echo "  export AWS_SECRET_ACCESS_KEY=..."
    echo "  export AWS_DEFAULT_REGION=us-west-2"
    echo ""
    echo "方式2: 使用 AWS Profile"
    echo "  export AWS_PROFILE=default"
    echo ""
    echo "方式3: 配置 AWS CLI"
    echo "  aws configure"
    echo ""
    
    echo "🔍 当前仍会显示错误信息（预期的）:"
    timeout 5s ./target/release/vector --config test_system_tables_simple.yaml 2>&1 | \
    grep -E "(AssumeRole|requires basic|Configuration error)" | head -5 || echo "错误显示完成"
fi

echo ""
echo "=== 调试完成 ==="
