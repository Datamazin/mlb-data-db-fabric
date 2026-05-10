# Deploy MLB Analytics to Azure Container Apps
# This script creates all required Azure resources and deploys the containerized Streamlit app

param(
    [string]$ResourceGroup = "mlb-analytics-rg",
    [string]$Location = "eastus",
    [string]$AcrName = "mlbanalyticsacr",
    [string]$AppName = "mlb-analytics",
    [string]$EnvName = "mlb-analytics-env"
)

Write-Host "=================================================" -ForegroundColor Green
Write-Host "Deploying MLB Analytics to Azure Container Apps" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
Write-Host ""

# Load environment variables from .env
Write-Host "Loading configuration from .env..." -ForegroundColor Cyan
if (Test-Path .env) {
    Get-Content .env | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]*)\s*=\s*(.*)$') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            Set-Item -Path "env:$name" -Value $value
        }
    }
    Write-Host "[OK] Loaded configuration" -ForegroundColor Green
}
else {
    Write-Host "[ERROR] .env file not found!" -ForegroundColor Red
    exit 1
}

# Verify required environment variables
$required = @('FABRIC_SERVER', 'FABRIC_DATABASE', 'FABRIC_AUTH', 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET', 'AZURE_TENANT_ID')
$missing = @()
foreach ($var in $required) {
    if (-not (Test-Path "env:$var")) {
        $missing += $var
    }
}
if ($missing.Count -gt 0) {
    Write-Host "[ERROR] Missing required environment variables: $($missing -join ', ')" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Step 1: Create Resource Group
Write-Host "[1/5] Creating Resource Group..." -ForegroundColor Cyan
az group create --name $ResourceGroup --location $Location --output table
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to create resource group" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Resource group created" -ForegroundColor Green
Write-Host ""

# Step 2: Create Azure Container Registry
Write-Host "[2/5] Creating Azure Container Registry..." -ForegroundColor Cyan
az acr create `
    --resource-group $ResourceGroup `
    --name $AcrName `
    --sku Basic `
    --admin-enabled true `
    --output table
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to create ACR" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Container Registry created" -ForegroundColor Green
Write-Host ""

# Step 3: Build and Push Image to ACR
Write-Host "[3/5] Building and pushing Docker image to ACR (this may take 5-10 minutes)..." -ForegroundColor Cyan
az acr build `
    --registry $AcrName `
    --image mlb-analytics:latest `
    --file Dockerfile `
    .
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to build/push image" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Image built and pushed" -ForegroundColor Green
Write-Host ""

# Step 4: Create Container Apps Environment
Write-Host "[4/5] Creating Container Apps Environment..." -ForegroundColor Cyan
az containerapp env create `
    --name $EnvName `
    --resource-group $ResourceGroup `
    --location $Location `
    --output table
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to create environment" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Environment created" -ForegroundColor Green
Write-Host ""

# Step 5: Deploy Container App
Write-Host "[5/5] Deploying Container App with secrets..." -ForegroundColor Cyan
az containerapp create `
    --name $AppName `
    --resource-group $ResourceGroup `
    --environment $EnvName `
    --image "$AcrName.azurecr.io/mlb-analytics:latest" `
    --registry-server "$AcrName.azurecr.io" `
    --target-port 8501 `
    --ingress external `
    --cpu 0.5 --memory 1.0Gi `
    --min-replicas 0 --max-replicas 3 `
    --secrets `
        fabric-server="$env:FABRIC_SERVER" `
        fabric-database="$env:FABRIC_DATABASE" `
        azure-client-id="$env:AZURE_CLIENT_ID" `
        azure-client-secret="$env:AZURE_CLIENT_SECRET" `
        azure-tenant-id="$env:AZURE_TENANT_ID" `
        onelake-workspace-id="$env:ONELAKE_WORKSPACE_ID" `
        onelake-workspace-name="$env:ONELAKE_WORKSPACE_NAME" `
        onelake-lakehouse-name="$env:ONELAKE_LAKEHOUSE_NAME" `
    --env-vars `
        FABRIC_SERVER=secretref:fabric-server `
        FABRIC_DATABASE=secretref:fabric-database `
        FABRIC_AUTH=ActiveDirectoryServicePrincipal `
        AZURE_CLIENT_ID=secretref:azure-client-id `
        AZURE_CLIENT_SECRET=secretref:azure-client-secret `
        AZURE_TENANT_ID=secretref:azure-tenant-id `
        ONELAKE_WORKSPACE_ID=secretref:onelake-workspace-id `
        ONELAKE_WORKSPACE_NAME=secretref:onelake-workspace-name `
        ONELAKE_LAKEHOUSE_NAME=secretref:onelake-lakehouse-name `
        MLB_API_BASE_URL="$env:MLB_API_BASE_URL" `
        MLB_API_RATE_LIMIT_RPS="$env:MLB_API_RATE_LIMIT_RPS" `
        MLB_API_MAX_RETRIES="$env:MLB_API_MAX_RETRIES" `
        LOG_LEVEL="$env:LOG_LEVEL" `
    --output table

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to deploy app" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] App deployed successfully" -ForegroundColor Green
Write-Host ""

# Step 6: Get the App URL
Write-Host "Getting app URL..." -ForegroundColor Cyan
$appUrl = az containerapp show `
    --name $AppName `
    --resource-group $ResourceGroup `
    --query "properties.configuration.ingress.fqdn" `
    --output tsv

Write-Host ""
Write-Host "=================================================" -ForegroundColor Green
Write-Host "           DEPLOYMENT COMPLETE!" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Your MLB Analytics app is now live at:" -ForegroundColor Cyan
Write-Host "  https://$appUrl" -ForegroundColor Yellow
Write-Host ""
Write-Host "Monitor your app:" -ForegroundColor Cyan
Write-Host "  az containerapp logs show --name $AppName --resource-group $ResourceGroup --follow" -ForegroundColor Gray
Write-Host ""
Write-Host "Update your app (after code changes):" -ForegroundColor Cyan
Write-Host "  az acr build --registry $AcrName --image mlb-analytics:latest ." -ForegroundColor Gray
Write-Host "  az containerapp update --name $AppName --resource-group $ResourceGroup --image `"$AcrName.azurecr.io/mlb-analytics:latest`"" -ForegroundColor Gray
Write-Host ""
Write-Host "Estimated cost: ~`$20-50/month (with auto-scale to zero)" -ForegroundColor Cyan
Write-Host ""
