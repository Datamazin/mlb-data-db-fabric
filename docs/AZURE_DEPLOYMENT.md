# Deployment Guide: MLB Analytics to Azure Container Apps

## Prerequisites

1. Azure CLI installed and logged in
2. Docker installed (for local testing)
3. Service Principal credentials (already in your `.env`)

## Step 1: Test Docker Build Locally

```bash
# Build the image
docker build -t mlb-analytics:latest .

# Test locally with your credentials
docker run -p 8501:8501 \
  -e FABRIC_SERVER="niagdix6nenejo5yletqutjpae-wa7bqeuqvb5e5grnhhkkzyaslu.database.fabric.microsoft.com" \
  -e FABRIC_DATABASE="mlb_db-4d0ed16a-a3cf-40ab-bff4-001267095080" \
  -e FABRIC_AUTH="ActiveDirectoryServicePrincipal" \
  -e AZURE_CLIENT_ID="ace7cf01-5308-4fee-b388-c55b0d7311e1" \
  -e AZURE_CLIENT_SECRET="<YOUR_AZURE_CLIENT_SECRET>" \
  -e AZURE_TENANT_ID="a261006a-69fe-441a-bbb8-59270a4d2f01" \
  mlb-analytics:latest

# Visit http://localhost:8501
```

## Step 2: Deploy to Azure Container Apps

```bash
# Set variables
RESOURCE_GROUP="mlb-analytics-rg"
LOCATION="eastus"  # Use same region as your Fabric Workspace
ACR_NAME="mlbanalyticsacr"
APP_NAME="mlb-analytics"

# Create resource group
az group create --name $RESOURCE_GROUP --location $LOCATION

# Create Azure Container Registry
az acr create --resource-group $RESOURCE_GROUP \
  --name $ACR_NAME --sku Basic --admin-enabled true

# Build and push image to ACR
az acr build --registry $ACR_NAME --image mlb-analytics:latest .

# Create Container Apps environment
az containerapp env create \
  --name mlb-analytics-env \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION

# Deploy Container App with secrets
az containerapp create \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --environment mlb-analytics-env \
  --image $ACR_NAME.azurecr.io/mlb-analytics:latest \
  --registry-server $ACR_NAME.azurecr.io \
  --target-port 8501 \
  --ingress external \
  --secrets \
    fabric-server="niagdix6nenejo5yletqutjpae-wa7bqeuqvb5e5grnhhkkzyaslu.database.fabric.microsoft.com" \
    fabric-database="mlb_db-4d0ed16a-a3cf-40ab-bff4-001267095080" \
    azure-client-id="ace7cf01-5308-4fee-b388-c55b0d7311e1" \
    azure-client-secret="<YOUR_AZURE_CLIENT_SECRET>" \
    azure-tenant-id="a261006a-69fe-441a-bbb8-59270a4d2f01" \
  --env-vars \
    FABRIC_SERVER=secretref:fabric-server \
    FABRIC_DATABASE=secretref:fabric-database \
    FABRIC_AUTH=ActiveDirectoryServicePrincipal \
    AZURE_CLIENT_ID=secretref:azure-client-id \
    AZURE_CLIENT_SECRET=secretref:azure-client-secret \
    AZURE_TENANT_ID=secretref:azure-tenant-id \
  --cpu 0.5 --memory 1.0Gi \
  --min-replicas 0 --max-replicas 3

# Get the app URL
az containerapp show \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --query properties.configuration.ingress.fqdn \
  --output tsv
```

## Step 3: Configure Auto-Scaling (Optional)

```bash
# Scale based on HTTP requests
az containerapp update \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --scale-rule-name http-rule \
  --scale-rule-type http \
  --scale-rule-http-concurrency 50
```

## Step 4: Set Up GitHub Actions (CI/CD)

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy to Azure Container Apps

on:
  push:
    branches: [main]
  workflow_dispatch:

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Login to Azure
        uses: azure/login@v1
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}
      
      - name: Build and push to ACR
        run: |
          az acr build \
            --registry mlbanalyticsacr \
            --image mlb-analytics:${{ github.sha }} \
            --image mlb-analytics:latest \
            .
      
      - name: Deploy to Container Apps
        run: |
          az containerapp update \
            --name mlb-analytics \
            --resource-group mlb-analytics-rg \
            --image mlbanalyticsacr.azurecr.io/mlb-analytics:${{ github.sha }}
```

## Estimated Costs

- **Container Apps**: ~$0.50-$2/day (with auto-scale to zero)
- **Container Registry**: ~$5/month (Basic tier)
- **Total**: ~$20-$70/month depending on traffic

## Alternative: Use Managed Identity (More Secure)

Instead of Service Principal, use Container Apps Managed Identity:

1. Enable Managed Identity on your Container App
2. Grant the identity permissions on Fabric Warehouse
3. Update `FABRIC_AUTH=ActiveDirectoryMsi`
4. Remove Service Principal secrets

This eliminates credential management entirely!
