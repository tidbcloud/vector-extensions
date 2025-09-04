#!/bin/bash

echo "🔍 Checking network connectivity and AWS service accessibility:"
echo ""

echo "1. Checking internet connection:"
if ping -c 2 8.8.8.8 > /dev/null 2>&1; then
    echo "✅ Internet connection is normal"
else
    echo "❌ Internet connection failed"
fi

echo ""
echo "2. Checking AWS STS service DNS resolution:"
if nslookup sts.us-west-2.amazonaws.com > /dev/null 2>&1; then
    echo "✅ AWS STS DNS resolution is normal"
else
    echo "❌ AWS STS DNS resolution failed"
fi

echo ""
echo "3. Checking AWS STS service connectivity:"
if curl -s --max-time 5 https://sts.us-west-2.amazonaws.com > /dev/null 2>&1; then
    echo "✅ AWS STS service is reachable"
else
    echo "❌ AWS STS service is not reachable"
fi

echo ""
echo "4. Checking AWS credentials environment:"
echo "AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-❌ Not set}"
echo "AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-❌ Not set}"
echo "AWS_PROFILE: ${AWS_PROFILE:-Not set}"
echo "AWS config file: $(ls ~/.aws/credentials 2>/dev/null && echo "✅ Exists" || echo "❌ Does not exist")"

echo ""
echo "5. Diagnostic conclusion:"
if ping -c 1 8.8.8.8 > /dev/null 2>&1; then
    if curl -s --max-time 3 https://sts.us-west-2.amazonaws.com > /dev/null 2>&1; then
        echo "🟢 Network connection is normal, AWS STS is reachable"
        echo "   - dispatch failure may be due to missing valid AWS credentials"
        echo "   - Recommend setting correct AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
    else
        echo "🟡 Network connection is normal, but AWS STS is not reachable"
        echo "   - May be a firewall or proxy issue"
        echo "   - May be an AWS service region issue"
    fi
else
    echo "🔴 Network connection problem"
    echo "   - Check internet connection"
    echo "   - Check network configuration"
fi
