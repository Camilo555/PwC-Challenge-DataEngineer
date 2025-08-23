# PwC Retail Data Platform - Terraform Makefile
# Provides convenient targets for common Terraform operations

.PHONY: help init plan apply destroy clean format validate lint
.DEFAULT_GOAL := help

# Configuration
PROJECT_NAME := pwc-retail-data-platform
TERRAFORM_VERSION := 1.5.0

# Environment validation
ifndef ENV
$(error ENV is not set. Usage: make plan ENV=dev)
endif

# Validate environment values
VALID_ENVS := dev staging prod
ifeq ($(filter $(ENV),$(VALID_ENVS)),)
$(error ENV must be one of: $(VALID_ENVS))
endif

# Cloud provider (default to aws)
CLOUD_PROVIDER ?= aws
VALID_PROVIDERS := aws azure gcp
ifeq ($(filter $(CLOUD_PROVIDER),$(VALID_PROVIDERS)),)
$(error CLOUD_PROVIDER must be one of: $(VALID_PROVIDERS))
endif

# Paths
ENV_DIR := environments/$(ENV)
TFVARS_FILE := $(ENV_DIR)/terraform.tfvars
PLAN_FILE := tfplan-$(ENV)

# Colors
CYAN := \033[0;36m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(CYAN)PwC Retail Data Platform - Terraform Makefile$(NC)"
	@echo "================================================="
	@echo ""
	@echo "$(YELLOW)Usage:$(NC)"
	@echo "  make <target> ENV=<environment> [CLOUD_PROVIDER=<provider>]"
	@echo ""
	@echo "$(YELLOW)Environments:$(NC) dev, staging, prod"
	@echo "$(YELLOW)Cloud Providers:$(NC) aws, azure, gcp (default: aws)"
	@echo ""
	@echo "$(YELLOW)Targets:$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(CYAN)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(YELLOW)Examples:$(NC)"
	@echo "  make plan ENV=dev"
	@echo "  make apply ENV=staging CLOUD_PROVIDER=aws"
	@echo "  make destroy ENV=dev"

check-env:
	@echo "$(CYAN)Environment Check$(NC)"
	@echo "Environment: $(YELLOW)$(ENV)$(NC)"
	@echo "Cloud Provider: $(YELLOW)$(CLOUD_PROVIDER)$(NC)"
	@if [ ! -d "$(ENV_DIR)" ]; then \
		echo "$(RED)Error: Environment directory not found: $(ENV_DIR)$(NC)"; \
		exit 1; \
	fi
	@if [ ! -f "$(TFVARS_FILE)" ]; then \
		echo "$(RED)Error: Variables file not found: $(TFVARS_FILE)$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)✓ Environment configuration found$(NC)"

init: check-env ## Initialize Terraform
	@echo "$(CYAN)Initializing Terraform...$(NC)"
	terraform init -upgrade

validate: init ## Validate Terraform configuration
	@echo "$(CYAN)Validating Terraform configuration...$(NC)"
	terraform validate

format: ## Format Terraform files
	@echo "$(CYAN)Formatting Terraform files...$(NC)"
	terraform fmt -recursive

lint: format validate ## Lint Terraform configuration
	@echo "$(CYAN)Linting completed$(NC)"

plan: lint ## Create Terraform execution plan
	@echo "$(CYAN)Creating Terraform plan for $(ENV)...$(NC)"
	terraform plan -var-file="$(TFVARS_FILE)" -out="$(PLAN_FILE)"

apply: plan ## Apply Terraform changes
	@echo "$(CYAN)Applying Terraform changes for $(ENV)...$(NC)"
	terraform apply "$(PLAN_FILE)"
	@echo "$(GREEN)✓ Deployment completed$(NC)"
	@make output

apply-auto: plan ## Apply Terraform changes without confirmation
	@echo "$(CYAN)Auto-applying Terraform changes for $(ENV)...$(NC)"
	terraform apply -auto-approve "$(PLAN_FILE)"
	@echo "$(GREEN)✓ Deployment completed$(NC)"
	@make output

destroy: check-env ## Destroy Terraform-managed infrastructure
	@echo "$(RED)⚠️  WARNING: This will destroy all infrastructure for $(ENV)!$(NC)"
	@read -p "Are you sure? (yes/no): " confirm && [ "$$confirm" = "yes" ]
	terraform plan -destroy -var-file="$(TFVARS_FILE)" -out="$(PLAN_FILE)-destroy"
	terraform apply -auto-approve "$(PLAN_FILE)-destroy"

output: ## Show Terraform outputs
	@echo "$(CYAN)Terraform Outputs:$(NC)"
	terraform output

state-list: ## List Terraform state resources
	@echo "$(CYAN)Terraform State Resources:$(NC)"
	terraform state list

state-show: ## Show detailed state information
	@echo "$(CYAN)Terraform State Details:$(NC)"
	terraform show

clean: ## Clean up temporary files
	@echo "$(CYAN)Cleaning up temporary files...$(NC)"
	rm -f tfplan-*
	rm -f .terraform.lock.hcl
	rm -rf .terraform/
	@echo "$(GREEN)✓ Cleanup completed$(NC)"

refresh: check-env ## Refresh Terraform state
	@echo "$(CYAN)Refreshing Terraform state...$(NC)"
	terraform refresh -var-file="$(TFVARS_FILE)"

import: ## Import existing resource (usage: make import ENV=dev RESOURCE=aws_instance.example ID=i-12345)
	@if [ -z "$(RESOURCE)" ] || [ -z "$(ID)" ]; then \
		echo "$(RED)Error: RESOURCE and ID must be specified$(NC)"; \
		echo "Usage: make import ENV=dev RESOURCE=aws_instance.example ID=i-12345"; \
		exit 1; \
	fi
	terraform import -var-file="$(TFVARS_FILE)" $(RESOURCE) $(ID)

setup-backend: ## Setup Terraform backend for state management
	@echo "$(CYAN)Setting up Terraform backend for $(ENV)...$(NC)"
	@if [ -f "scripts/setup-backend.sh" ]; then \
		chmod +x scripts/setup-backend.sh; \
		scripts/setup-backend.sh -e $(ENV); \
	else \
		echo "$(RED)Error: setup-backend.sh not found$(NC)"; \
	fi

graph: ## Generate Terraform dependency graph
	@echo "$(CYAN)Generating Terraform dependency graph...$(NC)"
	terraform graph | dot -Tpng > terraform-graph-$(ENV).png
	@echo "$(GREEN)✓ Graph saved to terraform-graph-$(ENV).png$(NC)"

cost-estimate: plan ## Estimate infrastructure costs (requires Infracost)
	@echo "$(CYAN)Estimating infrastructure costs...$(NC)"
	@if command -v infracost >/dev/null 2>&1; then \
		infracost breakdown --path . --terraform-plan-path $(PLAN_FILE); \
	else \
		echo "$(YELLOW)Infracost not found. Install it for cost estimation.$(NC)"; \
	fi

security-scan: ## Run security scan on Terraform code
	@echo "$(CYAN)Running security scan...$(NC)"
	@if command -v checkov >/dev/null 2>&1; then \
		checkov -d . --framework terraform; \
	elif command -v tfsec >/dev/null 2>&1; then \
		tfsec .; \
	else \
		echo "$(YELLOW)No security scanner found. Install checkov or tfsec.$(NC)"; \
	fi

docs: ## Generate Terraform documentation
	@echo "$(CYAN)Generating Terraform documentation...$(NC)"
	@if command -v terraform-docs >/dev/null 2>&1; then \
		terraform-docs markdown table --output-file README.md .; \
		echo "$(GREEN)✓ Documentation generated$(NC)"; \
	else \
		echo "$(YELLOW)terraform-docs not found. Install it for documentation generation.$(NC)"; \
	fi

# Composite targets
deploy: apply ## Alias for apply
redeploy: destroy apply ## Destroy and recreate infrastructure
full-check: lint security-scan cost-estimate ## Run all checks