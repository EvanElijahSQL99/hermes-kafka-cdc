#!/usr/bin/env bash
# Creates the Hermes demo on AWS: one EC2 instance running docker compose, behind CloudFront (HTTPS).
# Run it in AWS CloudShell in the region you want (default us-east-2). Safe to re-run: it reuses what exists.
#
#   bash deploy/aws-setup.sh
#
# What it creates (all tagged Name=hermes):
#   - security group: port 80 open ONLY to CloudFront's origin-facing IP ranges (no SSH; admin is via SSM)
#   - IAM role + instance profile with AmazonSSMManagedInstanceCore
#   - t3.medium Amazon Linux 2023 instance, 30 GB gp3, with an Elastic IP
#   - CloudFront distribution: HTTPS for viewers, no caching for the app, long caching for /assets/*
set -euo pipefail

REGION="${AWS_REGION:-us-east-2}"
REPO_URL="${REPO_URL:-https://github.com/EvanElijahSQL99/hermes-kafka-cdc.git}"
TYPE="${INSTANCE_TYPE:-t3.medium}"
NAME=hermes
export AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION" AWS_PAGER=""
say() { printf '\n== %s\n' "$*"; }

say "Network (default VPC in $REGION)"
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
[ "$VPC" = "None" ] && { echo "No default VPC in $REGION. Create one with: aws ec2 create-default-vpc"; exit 1; }
SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true \
  --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text)
echo "vpc $VPC, subnet $SUBNET"

say "Security group (HTTP from CloudFront only)"
SG=$(aws ec2 describe-security-groups --filters Name=vpc-id,Values="$VPC" Name=group-name,Values=hermes-web \
  --query 'SecurityGroups[0].GroupId' --output text)
if [ "$SG" = "None" ]; then
  SG=$(aws ec2 create-security-group --group-name hermes-web --vpc-id "$VPC" \
    --description "Hermes: HTTP from CloudFront origin-facing ranges only" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=$NAME}]" --query GroupId --output text)
  PL=$(aws ec2 describe-managed-prefix-lists --filters Name=prefix-list-name,Values=com.amazonaws.global.cloudfront.origin-facing \
    --query 'PrefixLists[0].PrefixListId' --output text)
  aws ec2 authorize-security-group-ingress --group-id "$SG" \
    --ip-permissions "IpProtocol=tcp,FromPort=80,ToPort=80,PrefixListIds=[{PrefixListId=$PL,Description=CloudFront}]" >/dev/null
fi
echo "security group $SG"

say "IAM role for Systems Manager (no SSH keys needed)"
if ! aws iam get-role --role-name hermes-ec2 >/dev/null 2>&1; then
  aws iam create-role --role-name hermes-ec2 --assume-role-policy-document \
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  aws iam attach-role-policy --role-name hermes-ec2 --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
fi
if ! aws iam get-instance-profile --instance-profile-name hermes-ec2 >/dev/null 2>&1; then
  aws iam create-instance-profile --instance-profile-name hermes-ec2 >/dev/null
  aws iam add-role-to-instance-profile --instance-profile-name hermes-ec2 --role-name hermes-ec2
  sleep 10   # instance profiles take a moment to become usable
fi

say "EC2 instance"
IID=$(aws ec2 describe-instances --filters Name=tag:Name,Values=$NAME Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)
if [ "$IID" = "None" ]; then
  AMI=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query Parameter.Value --output text)
  USERDATA=$(mktemp)
  cat > "$USERDATA" <<EOF
#!/bin/bash
set -euxo pipefail
# 2 GB swap: headroom for Kafka, Kafka Connect and the image build on a 4 GB host
fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab
dnf install -y docker git
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL -o /usr/local/lib/docker/cli-plugins/docker-compose https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64
BX=\$(curl -fsSL https://api.github.com/repos/docker/buildx/releases/latest | grep -m1 '"tag_name"' | cut -d'"' -f4)
curl -fsSL -o /usr/local/lib/docker/cli-plugins/docker-buildx "https://github.com/docker/buildx/releases/download/\$BX/buildx-\$BX.linux-amd64"
chmod +x /usr/local/lib/docker/cli-plugins/*
systemctl enable --now docker
git clone $REPO_URL /opt/hermes
cd /opt/hermes
HERMES_PORT=80 docker compose up -d --build
EOF
  IID=$(aws ec2 run-instances --image-id "$AMI" --instance-type "$TYPE" --subnet-id "$SUBNET" --security-group-ids "$SG" \
    --iam-instance-profile Name=hermes-ec2 --user-data "file://$USERDATA" \
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=30,VolumeType=gp3}' \
    --metadata-options HttpTokens=required \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME}]" "ResourceType=volume,Tags=[{Key=Name,Value=$NAME}]" \
    --query 'Instances[0].InstanceId' --output text)
  rm -f "$USERDATA"
fi
echo "instance $IID"
aws ec2 wait instance-running --instance-ids "$IID"

say "Elastic IP"
ALLOC=$(aws ec2 describe-addresses --filters Name=tag:Name,Values=$NAME --query 'Addresses[0].AllocationId' --output text)
if [ "$ALLOC" = "None" ]; then
  ALLOC=$(aws ec2 allocate-address --domain vpc --tag-specifications "ResourceType=elastic-ip,Tags=[{Key=Name,Value=$NAME}]" \
    --query AllocationId --output text)
fi
aws ec2 associate-address --allocation-id "$ALLOC" --instance-id "$IID" --allow-reassociation >/dev/null
sleep 5
ORIGIN=$(aws ec2 describe-instances --instance-ids "$IID" --query 'Reservations[0].Instances[0].PublicDnsName' --output text)
echo "origin $ORIGIN"

say "CloudFront distribution"
DIST=$(aws cloudfront list-distributions --query "DistributionList.Items[?Comment=='hermes'].Id | [0]" --output text)
if [ "$DIST" = "None" ] || [ -z "$DIST" ]; then
  CFG=$(mktemp)
  cat > "$CFG" <<EOF
{
  "CallerReference": "hermes-$(date +%s)",
  "Comment": "hermes",
  "Enabled": true,
  "HttpVersion": "http2and3",
  "PriceClass": "PriceClass_100",
  "Origins": { "Quantity": 1, "Items": [{
    "Id": "hermes-ec2", "DomainName": "$ORIGIN",
    "CustomOriginConfig": { "HTTPPort": 80, "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
      "OriginReadTimeout": 60, "OriginKeepaliveTimeout": 60,
      "OriginSslProtocols": { "Quantity": 1, "Items": ["TLSv1.2"] } } }] },
  "DefaultCacheBehavior": {
    "TargetOriginId": "hermes-ec2", "ViewerProtocolPolicy": "redirect-to-https", "Compress": false,
    "CachePolicyId": "4135ea2d-6df8-44a3-9df3-4b5a84be39ad",
    "OriginRequestPolicyId": "216adef6-5c7f-47e4-b989-5492eafa07d3",
    "AllowedMethods": { "Quantity": 7, "Items": ["GET","HEAD","OPTIONS","PUT","PATCH","POST","DELETE"],
      "CachedMethods": { "Quantity": 2, "Items": ["GET","HEAD"] } } },
  "CacheBehaviors": { "Quantity": 1, "Items": [{
    "PathPattern": "/assets/*", "TargetOriginId": "hermes-ec2", "ViewerProtocolPolicy": "redirect-to-https", "Compress": true,
    "CachePolicyId": "658327ea-f89d-4fab-a63d-7e88639e58f6",
    "AllowedMethods": { "Quantity": 2, "Items": ["GET","HEAD"], "CachedMethods": { "Quantity": 2, "Items": ["GET","HEAD"] } } }] }
}
EOF
  DIST=$(aws cloudfront create-distribution --distribution-config "file://$CFG" --query Distribution.Id --output text)
  rm -f "$CFG"
fi
DOMAIN=$(aws cloudfront get-distribution --id "$DIST" --query Distribution.DomainName --output text)

say "Done"
cat <<EOF
Instance:     $IID  (first boot builds the images; allow about 10 minutes)
CloudFront:   $DIST  (deploys in about 5 minutes)
Site:         https://$DOMAIN

Check progress on the instance:
  aws ssm send-command --instance-ids $IID --document-name AWS-RunShellScript \\
    --parameters 'commands=["docker ps --format \"{{.Names}} {{.Status}}\""]' --query Command.CommandId --output text
EOF
