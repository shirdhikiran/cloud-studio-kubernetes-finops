from azure.identity import DefaultAzureCredential
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.resource import ResourceManagementClient

from datetime import datetime, timedelta, timezone
import re
import json

# Setup auth
credential = DefaultAzureCredential()
subscription_id = "5785f902-e9bc-4b4c-8e09-2732a083cdd4"

# Clients
cost_client = CostManagementClient(credential)
resource_client = ResourceManagementClient(credential, subscription_id)

# Date range
end_date = datetime.now(timezone.utc)
start_date = end_date - timedelta(days=30)

# Cost query scope
scope = f"/subscriptions/{subscription_id}"

# Define query
query = {
    "type": "Usage",
    "timeframe": "Custom",
    "timePeriod": {
        "from": start_date.isoformat(),
        "to": end_date.isoformat()
    },
    "dataset": {
        "granularity": "None",
        "aggregation": {
            "cost": {
                "name": "PreTaxCost",
                "function": "Sum"
            }
        },
        "grouping": [
            {"type": "Dimension", "name": "ResourceId"}
        ]
    }
}

# Run cost query
print("Querying cost data...")
result = cost_client.query.usage(scope, query)
import pdb; pdb.set_trace()
# Collect all AKS-related resources
aks_resource_costs = {}

for row in result.rows:
    resource_id = row[0]
    cost = row[1]

    # Skip blank or malformed
    if not resource_id or not isinstance(resource_id, str):
        continue

    # Detect AKS-related resources (by common patterns)
    if "Microsoft.ContainerService/managedClusters" in resource_id:
        cluster_name = resource_id.split("/")[-1]
    elif "MC_" in resource_id:
        # Extract cluster name from MC_ resource group naming
        match = re.search(r"/resourceGroups/MC_([^/]+)/", resource_id)
        cluster_name = match.group(1) if match else "unknown"
    else:
        continue

    if cluster_name not in aks_resource_costs:
        aks_resource_costs[cluster_name] = []

    aks_resource_costs[cluster_name].append({
        "resource_id": resource_id,
        "cost": round(cost, 2)
    })

# Display
print("\n===== AKS Cluster Cost Breakdown (Last 30 Days) =====\n")
for cluster, resources in aks_resource_costs.items():
    total = sum(r["cost"] for r in resources)
    print(f"\n🔹 Cluster: {cluster} — ₹{total:.2f}")
    for r in sorted(resources, key=lambda x: -x["cost"]):
        print(f"   - {r['resource_id']} — ₹{r['cost']}")
