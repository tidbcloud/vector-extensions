#!/bin/bash

echo "🔍 检查网络连通性和 AWS 服务可达性："
echo ""

echo "1. 检查互联网连接："
if ping -c 2 8.8.8.8 > /dev/null 2>&1; then
    echo "✅ 互联网连接正常"
else
    echo "❌ 互联网连接失败"
fi

echo ""
echo "2. 检查 AWS STS 服务 DNS 解析："
if nslookup sts.us-west-2.amazonaws.com > /dev/null 2>&1; then
    echo "✅ AWS STS DNS 解析正常"
else
    echo "❌ AWS STS DNS 解析失败"
fi

echo ""
echo "3. 检查 AWS STS 服务连通性："
if curl -s --max-time 5 https://sts.us-west-2.amazonaws.com > /dev/null 2>&1; then
    echo "✅ AWS STS 服务可达"
else
    echo "❌ AWS STS 服务不可达"
fi

echo ""
echo "4. 检查 AWS 凭证环境："
echo "AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-❌ 未设置}"
echo "AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-❌ 未设置}"
echo "AWS_PROFILE: ${AWS_PROFILE:-未设置}"
echo "AWS 配置文件: $(ls ~/.aws/credentials 2>/dev/null && echo "✅ 存在" || echo "❌ 不存在")"

echo ""
echo "5. 诊断结论："
if ping -c 1 8.8.8.8 > /dev/null 2>&1; then
    if curl -s --max-time 3 https://sts.us-west-2.amazonaws.com > /dev/null 2>&1; then
        echo "🟢 网络连接正常，AWS STS 可达"
        echo "   - dispatch failure 可能是由于缺少有效的 AWS 凭证"
        echo "   - 建议设置正确的 AWS_ACCESS_KEY_ID 和 AWS_SECRET_ACCESS_KEY"
    else
        echo "🟡 网络连接正常，但 AWS STS 不可达"
        echo "   - 可能是防火墙或代理问题"
        echo "   - 可能是 AWS 服务区域问题"
    fi
else
    echo "🔴 网络连接问题"
    echo "   - 检查互联网连接"
    echo "   - 检查网络配置"
fi
