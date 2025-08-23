#!/bin/bash
# PwC Retail Data Platform - Terraform Backend Setup Script
# Sets up S3 backend for Terraform state management

set -e

# Configuration
PROJECT_NAME="pwc-retail-data-platform"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default values
REGION="us-west-2"
ENVIRONMENT=""

usage() {
    echo "Usage: $0 -e <environment> [OPTIONS]"
    echo ""
    echo "Required arguments:"
    echo "  -e, --environment    Environment (dev|staging|prod)"
    echo ""
    echo "Optional arguments:"
    echo "  -r, --region         AWS region [default: us-west-2]"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -e dev"
    echo "  $0 -e prod -r us-east-1"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -r|--region)
            REGION="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$ENVIRONMENT" ]]; then
    echo -e "${RED}Error: Environment is required${NC}"
    usage
    exit 1
fi

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
    echo -e "${RED}Error: Environment must be dev, staging, or prod${NC}"
    exit 1
fi

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo -e "${RED}Error: AWS CLI not found. Please install AWS CLI.${NC}"
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}Error: AWS credentials not configured. Please run 'aws configure'.${NC}"
    exit 1
fi

# Configuration
BUCKET_NAME="${PROJECT_NAME}-terraform-state-${ENVIRONMENT}"
DYNAMODB_TABLE="${PROJECT_NAME}-terraform-locks-${ENVIRONMENT}"

echo -e "${CYAN}=================================================${NC}"
echo -e "${CYAN}PwC Retail Data Platform - Backend Setup${NC}"
echo -e "${CYAN}=================================================${NC}"
echo -e "${YELLOW}Environment: $ENVIRONMENT${NC}"
echo -e "${YELLOW}Region: $REGION${NC}"
echo -e "${YELLOW}S3 Bucket: $BUCKET_NAME${NC}"
echo -e "${YELLOW}DynamoDB Table: $DYNAMODB_TABLE${NC}"

# Create S3 bucket for state
echo -e "\n${CYAN}--- Creating S3 Bucket for Terraform State ---${NC}"
if aws s3api head-bucket --bucket "$BUCKET_NAME" 2>/dev/null; then
    echo -e "${YELLOW}S3 bucket $BUCKET_NAME already exists${NC}"
else
    if [[ "$REGION" == "us-east-1" ]]; then
        aws s3api create-bucket --bucket "$BUCKET_NAME" --region "$REGION"
    else
        aws s3api create-bucket --bucket "$BUCKET_NAME" --region "$REGION" \
            --create-bucket-configuration LocationConstraint="$REGION"
    fi
    echo -e "${GREEN}Created S3 bucket: $BUCKET_NAME${NC}"
fi

# Enable versioning
echo -e "\n${CYAN}--- Enabling S3 Bucket Versioning ---${NC}"
aws s3api put-bucket-versioning \
    --bucket "$BUCKET_NAME" \
    --versioning-configuration Status=Enabled
echo -e "${GREEN}Enabled versioning for S3 bucket${NC}"

# Enable server-side encryption
echo -e "\n${CYAN}--- Enabling S3 Bucket Encryption ---${NC}"
aws s3api put-bucket-encryption \
    --bucket "$BUCKET_NAME" \
    --server-side-encryption-configuration '{
        "Rules": [
            {
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "AES256"
                }
            }
        ]
    }'
echo -e "${GREEN}Enabled encryption for S3 bucket${NC}"

# Block public access
echo -e "\n${CYAN}--- Blocking S3 Bucket Public Access ---${NC}"
aws s3api put-public-access-block \
    --bucket "$BUCKET_NAME" \
    --public-access-block-configuration \
        BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
echo -e "${GREEN}Blocked public access for S3 bucket${NC}"

# Create DynamoDB table for state locking
echo -e "\n${CYAN}--- Creating DynamoDB Table for State Locking ---${NC}"
if aws dynamodb describe-table --table-name "$DYNAMODB_TABLE" &>/dev/null; then
    echo -e "${YELLOW}DynamoDB table $DYNAMODB_TABLE already exists${NC}"
else
    aws dynamodb create-table \
        --table-name "$DYNAMODB_TABLE" \
        --attribute-definitions AttributeName=LockID,AttributeType=S \
        --key-schema AttributeName=LockID,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
        --region "$REGION"
    
    echo -e "${YELLOW}Waiting for DynamoDB table to be created...${NC}"
    aws dynamodb wait table-exists --table-name "$DYNAMODB_TABLE" --region "$REGION"
    echo -e "${GREEN}Created DynamoDB table: $DYNAMODB_TABLE${NC}"
fi

# Create backend configuration file
BACKEND_CONFIG_FILE="../environments/$ENVIRONMENT/backend.hcl"
echo -e "\n${CYAN}--- Creating Backend Configuration ---${NC}"

cat > "$BACKEND_CONFIG_FILE" << EOF
# Terraform Backend Configuration for $ENVIRONMENT
# Generated by setup-backend.sh

bucket         = "$BUCKET_NAME"
key            = "terraform.tfstate"
region         = "$REGION"
dynamodb_table = "$DYNAMODB_TABLE"
encrypt        = true

# Optional: Enable state locking
# lock_table = "$DYNAMODB_TABLE"
EOF

echo -e "${GREEN}Created backend configuration: $BACKEND_CONFIG_FILE${NC}"

# Display next steps
echo -e "\n${GREEN}--- Setup Complete ---${NC}"
echo -e "${GREEN}Backend setup completed successfully!${NC}"
echo -e "\n${CYAN}Next steps:${NC}"
echo -e "1. Initialize Terraform with the backend:"
echo -e "   ${YELLOW}terraform init -backend-config=environments/$ENVIRONMENT/backend.hcl${NC}"
echo -e ""
echo -e "2. Or add this to your main.tf terraform block:"
echo -e "${YELLOW}  backend \"s3\" {${NC}"
echo -e "${YELLOW}    bucket         = \"$BUCKET_NAME\"${NC}"
echo -e "${YELLOW}    key            = \"terraform.tfstate\"${NC}"
echo -e "${YELLOW}    region         = \"$REGION\"${NC}"
echo -e "${YELLOW}    dynamodb_table = \"$DYNAMODB_TABLE\"${NC}"
echo -e "${YELLOW}    encrypt        = true${NC}"
echo -e "${YELLOW}  }${NC}"