# PwC Retail Data Platform - Terraform Deployment Script
# PowerShell script for Windows environments

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("dev", "staging", "prod")]
    [string]$Environment,
    
    [Parameter(Mandatory=$false)]
    [ValidateSet("aws", "azure", "gcp")]
    [string]$CloudProvider = "aws",
    
    [Parameter(Mandatory=$false)]
    [switch]$PlanOnly,
    
    [Parameter(Mandatory=$false)]
    [switch]$AutoApprove,
    
    [Parameter(Mandatory=$false)]
    [switch]$Destroy
)

# Set error handling
$ErrorActionPreference = "Stop"

# Configuration
$PROJECT_NAME = "pwc-retail-data-platform"
$TERRAFORM_DIR = Split-Path -Parent $PSScriptRoot
$ENV_DIR = Join-Path $TERRAFORM_DIR "environments\$Environment"

Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "PwC Retail Data Platform - Terraform Deployment" -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "Environment: $Environment" -ForegroundColor Yellow
Write-Host "Cloud Provider: $CloudProvider" -ForegroundColor Yellow
Write-Host "Terraform Directory: $TERRAFORM_DIR" -ForegroundColor Yellow

# Check if environment directory exists
if (-not (Test-Path $ENV_DIR)) {
    Write-Host "Error: Environment directory not found: $ENV_DIR" -ForegroundColor Red
    exit 1
}

# Check if terraform.tfvars exists
$TFVARS_FILE = Join-Path $ENV_DIR "terraform.tfvars"
if (-not (Test-Path $TFVARS_FILE)) {
    Write-Host "Error: terraform.tfvars not found: $TFVARS_FILE" -ForegroundColor Red
    exit 1
}

# Set working directory
Set-Location $TERRAFORM_DIR
Write-Host "Changed to directory: $TERRAFORM_DIR" -ForegroundColor Green

# Check if Terraform is installed
try {
    $tf_version = terraform version
    Write-Host "Terraform version: $tf_version" -ForegroundColor Green
} catch {
    Write-Host "Error: Terraform not found. Please install Terraform." -ForegroundColor Red
    exit 1
}

# Initialize Terraform
Write-Host "`n--- Initializing Terraform ---" -ForegroundColor Cyan
terraform init -upgrade

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Terraform initialization failed" -ForegroundColor Red
    exit 1
}

# Validate configuration
Write-Host "`n--- Validating Terraform Configuration ---" -ForegroundColor Cyan
terraform validate

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Terraform validation failed" -ForegroundColor Red
    exit 1
}

# Format check
Write-Host "`n--- Checking Terraform Formatting ---" -ForegroundColor Cyan
terraform fmt -check -recursive

if ($LASTEXITCODE -ne 0) {
    Write-Host "Warning: Terraform files need formatting. Running 'terraform fmt'..." -ForegroundColor Yellow
    terraform fmt -recursive
}

# Plan
Write-Host "`n--- Creating Terraform Plan ---" -ForegroundColor Cyan
if ($Destroy) {
    $plan_args = @(
        "plan",
        "-destroy",
        "-var-file=$TFVARS_FILE",
        "-out=tfplan-destroy-$Environment"
    )
} else {
    $plan_args = @(
        "plan",
        "-var-file=$TFVARS_FILE",
        "-out=tfplan-$Environment"
    )
}

& terraform @plan_args

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Terraform planning failed" -ForegroundColor Red
    exit 1
}

# Exit if plan only
if ($PlanOnly) {
    Write-Host "`nPlan completed successfully. Use -AutoApprove to apply changes." -ForegroundColor Green
    exit 0
}

# Apply or Destroy
if ($Destroy) {
    Write-Host "`n--- Destroying Infrastructure ---" -ForegroundColor Red
    Write-Host "WARNING: This will destroy all infrastructure for $Environment environment!" -ForegroundColor Red
    
    if (-not $AutoApprove) {
        $confirmation = Read-Host "Are you sure you want to destroy? (yes/no)"
        if ($confirmation -ne "yes") {
            Write-Host "Destruction cancelled." -ForegroundColor Yellow
            exit 0
        }
    }
    
    terraform apply -auto-approve "tfplan-destroy-$Environment"
} else {
    Write-Host "`n--- Applying Terraform Changes ---" -ForegroundColor Cyan
    
    if ($AutoApprove) {
        terraform apply -auto-approve "tfplan-$Environment"
    } else {
        terraform apply "tfplan-$Environment"
    }
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Terraform apply failed" -ForegroundColor Red
    exit 1
}

# Show outputs
if (-not $Destroy) {
    Write-Host "`n--- Terraform Outputs ---" -ForegroundColor Cyan
    terraform output
    
    Write-Host "`n--- Next Steps ---" -ForegroundColor Green
    terraform output -raw next_steps | ForEach-Object { Write-Host $_ }
}

Write-Host "`nDeployment completed successfully!" -ForegroundColor Green