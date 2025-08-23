#!/bin/bash
# PwC Retail Data Platform - Terraform Deployment Script
# Bash script for Linux/macOS environments

set -e

# Default values
ENVIRONMENT=""
CLOUD_PROVIDER="aws"
PLAN_ONLY=false
AUTO_APPROVE=false
DESTROY=false

# Configuration
PROJECT_NAME="pwc-retail-data-platform"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 -e <environment> [OPTIONS]"
    echo ""
    echo "Required arguments:"
    echo "  -e, --environment    Environment (dev|staging|prod)"
    echo ""
    echo "Optional arguments:"
    echo "  -c, --cloud-provider Cloud provider (aws|azure|gcp) [default: aws]"
    echo "  -p, --plan-only      Only run terraform plan"
    echo "  -a, --auto-approve   Auto approve terraform apply"
    echo "  -d, --destroy        Destroy infrastructure"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -e dev"
    echo "  $0 -e staging -c aws -p"
    echo "  $0 -e prod -c aws -a"
    echo "  $0 -e dev -d"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -c|--cloud-provider)
            CLOUD_PROVIDER="$2"
            shift 2
            ;;
        -p|--plan-only)
            PLAN_ONLY=true
            shift
            ;;
        -a|--auto-approve)
            AUTO_APPROVE=true
            shift
            ;;
        -d|--destroy)
            DESTROY=true
            shift
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

# Validate cloud provider
if [[ ! "$CLOUD_PROVIDER" =~ ^(aws|azure|gcp)$ ]]; then
    echo -e "${RED}Error: Cloud provider must be aws, azure, or gcp${NC}"
    exit 1
fi

ENV_DIR="$TERRAFORM_DIR/environments/$ENVIRONMENT"

echo -e "${CYAN}=================================================${NC}"
echo -e "${CYAN}PwC Retail Data Platform - Terraform Deployment${NC}"
echo -e "${CYAN}=================================================${NC}"
echo -e "${YELLOW}Environment: $ENVIRONMENT${NC}"
echo -e "${YELLOW}Cloud Provider: $CLOUD_PROVIDER${NC}"
echo -e "${YELLOW}Terraform Directory: $TERRAFORM_DIR${NC}"

# Check if environment directory exists
if [[ ! -d "$ENV_DIR" ]]; then
    echo -e "${RED}Error: Environment directory not found: $ENV_DIR${NC}"
    exit 1
fi

# Check if terraform.tfvars exists
TFVARS_FILE="$ENV_DIR/terraform.tfvars"
if [[ ! -f "$TFVARS_FILE" ]]; then
    echo -e "${RED}Error: terraform.tfvars not found: $TFVARS_FILE${NC}"
    exit 1
fi

# Set working directory
cd "$TERRAFORM_DIR"
echo -e "${GREEN}Changed to directory: $TERRAFORM_DIR${NC}"

# Check if Terraform is installed
if ! command -v terraform &> /dev/null; then
    echo -e "${RED}Error: Terraform not found. Please install Terraform.${NC}"
    exit 1
fi

TF_VERSION=$(terraform version | head -n1)
echo -e "${GREEN}$TF_VERSION${NC}"

# Check if required cloud CLI is installed
case $CLOUD_PROVIDER in
    aws)
        if ! command -v aws &> /dev/null; then
            echo -e "${YELLOW}Warning: AWS CLI not found. Some features may not work.${NC}"
        fi
        ;;
    azure)
        if ! command -v az &> /dev/null; then
            echo -e "${YELLOW}Warning: Azure CLI not found. Some features may not work.${NC}"
        fi
        ;;
    gcp)
        if ! command -v gcloud &> /dev/null; then
            echo -e "${YELLOW}Warning: Google Cloud SDK not found. Some features may not work.${NC}"
        fi
        ;;
esac

# Initialize Terraform
echo -e "\n${CYAN}--- Initializing Terraform ---${NC}"
terraform init -upgrade

# Validate configuration
echo -e "\n${CYAN}--- Validating Terraform Configuration ---${NC}"
terraform validate

# Format check
echo -e "\n${CYAN}--- Checking Terraform Formatting ---${NC}"
if ! terraform fmt -check -recursive; then
    echo -e "${YELLOW}Warning: Terraform files need formatting. Running 'terraform fmt'...${NC}"
    terraform fmt -recursive
fi

# Plan
echo -e "\n${CYAN}--- Creating Terraform Plan ---${NC}"
if [[ "$DESTROY" == true ]]; then
    terraform plan -destroy -var-file="$TFVARS_FILE" -out="tfplan-destroy-$ENVIRONMENT"
else
    terraform plan -var-file="$TFVARS_FILE" -out="tfplan-$ENVIRONMENT"
fi

# Exit if plan only
if [[ "$PLAN_ONLY" == true ]]; then
    echo -e "\n${GREEN}Plan completed successfully. Use -a to apply changes.${NC}"
    exit 0
fi

# Apply or Destroy
if [[ "$DESTROY" == true ]]; then
    echo -e "\n${RED}--- Destroying Infrastructure ---${NC}"
    echo -e "${RED}WARNING: This will destroy all infrastructure for $ENVIRONMENT environment!${NC}"
    
    if [[ "$AUTO_APPROVE" != true ]]; then
        read -p "Are you sure you want to destroy? (yes/no): " confirmation
        if [[ "$confirmation" != "yes" ]]; then
            echo -e "${YELLOW}Destruction cancelled.${NC}"
            exit 0
        fi
    fi
    
    terraform apply -auto-approve "tfplan-destroy-$ENVIRONMENT"
else
    echo -e "\n${CYAN}--- Applying Terraform Changes ---${NC}"
    
    if [[ "$AUTO_APPROVE" == true ]]; then
        terraform apply -auto-approve "tfplan-$ENVIRONMENT"
    else
        terraform apply "tfplan-$ENVIRONMENT"
    fi
fi

# Show outputs
if [[ "$DESTROY" != true ]]; then
    echo -e "\n${CYAN}--- Terraform Outputs ---${NC}"
    terraform output
    
    echo -e "\n${GREEN}--- Next Steps ---${NC}"
    terraform output -raw next_steps | while IFS= read -r line; do
        [[ -n "$line" ]] && echo -e "${GREEN}$line${NC}"
    done
fi

echo -e "\n${GREEN}Deployment completed successfully!${NC}"