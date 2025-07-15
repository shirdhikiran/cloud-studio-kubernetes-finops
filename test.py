# Container Insights Testing - Step by Step

import asyncio
from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient
from datetime import datetime, timedelta, timezone
import os

class ContainerInsightsValidator:
    def __init__(self, workspace_id):
        self.workspace_id = workspace_id
        self.credential = DefaultAzureCredential()
        self.client = LogsQueryClient(self.credential)
    
    async def test_basic_connectivity(self):
        """Test 1: Basic connectivity to Log Analytics"""
        print("🔍 Test 1: Testing basic Log Analytics connectivity...")
        
        try:
            # Simple query that should always return something if workspace exists
            query = "Usage | limit 1"
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=1)
            
            response = self.client.query_workspace(
                workspace_id=self.workspace_id,
                query=query,
                timespan=(start_time, end_time)
            )
            
            if response.tables and response.tables[0].rows:
                print("✅ Workspace connectivity: SUCCESS")
                return True
            else:
                print("❌ Workspace connectivity: FAILED - No usage data")
                return False
                
        except Exception as e:
            print(f"❌ Workspace connectivity: FAILED - {e}")
            return False
    
    async def check_available_tables(self):
        """Test 2: Check which Container Insights tables exist"""
        print("\n🔍 Test 2: Checking available Container Insights tables...")
        
        container_insights_tables = [
            "Perf", "KubeNodeInventory", "KubePodInventory", 
            "ContainerInventory", "KubeServices", "KubeEvents",
            "ContainerLog", "InsightsMetrics"
        ]
        
        available_tables = []
        
        for table in container_insights_tables:
            try:
                query = f"{table} | limit 1"
                end_time = datetime.now(timezone.utc)
                start_time = end_time - timedelta(hours=1)
                
                response = self.client.query_workspace(
                    workspace_id=self.workspace_id,
                    query=query,
                    timespan=(start_time, end_time)
                )
                
                if response.tables:
                    available_tables.append(table)
                    print(f"✅ Table {table}: Available")
                else:
                    print(f"❌ Table {table}: Not available or no data")
                    
            except Exception as e:
                print(f"❌ Table {table}: Error - {e}")
        
        return available_tables
    
    async def test_time_ranges(self):
        """Test 3: Try different time ranges to find data"""
        print("\n🔍 Test 3: Testing different time ranges for Perf table...")
        
        time_ranges = [
            ("1 hour", timedelta(hours=1)),
            ("6 hours", timedelta(hours=6)), 
            ("1 day", timedelta(days=1)),
            ("3 days", timedelta(days=3)),
            ("7 days", timedelta(days=7)),
            ("30 days", timedelta(days=30))
        ]
        
        for range_name, delta in time_ranges:
            try:
                query = "Perf | where ObjectName == 'K8SNode' | limit 5"
                end_time = datetime.now(timezone.utc)
                start_time = end_time - delta
                
                response = self.client.query_workspace(
                    workspace_id=self.workspace_id,
                    query=query,
                    timespan=(start_time, end_time)
                )
                
                if response.tables and response.tables[0].rows:
                    row_count = len(response.tables[0].rows)
                    print(f"✅ {range_name}: Found {row_count} rows")
                    
                    # Show sample data
                    if row_count > 0:
                        print(f"   Sample row: {response.tables[0].rows[0]}")
                    return True
                else:
                    print(f"❌ {range_name}: No data")
                    
            except Exception as e:
                print(f"❌ {range_name}: Error - {e}")
        
        return False
    
    async def test_specific_queries(self):
        """Test 4: Try specific working queries"""
        print("\n🔍 Test 4: Testing specific Container Insights queries...")
        
        # These queries are more likely to return data
        test_queries = {
            "Any Container Insights data": "search * | where $table startswith 'Kube' or $table == 'Perf' | limit 5",
            "Heartbeat data": "Heartbeat | limit 5",
            "Any Kubernetes data": "union Kube* | limit 5", 
            "Container logs": "ContainerLog | limit 5",
            "Insights metrics": "InsightsMetrics | limit 5",
            "Performance counters": "Perf | limit 5"
        }
        
        working_queries = []
        
        for query_name, query in test_queries.items():
            try:
                end_time = datetime.now(timezone.utc)
                start_time = end_time - timedelta(days=7)  # Try 7 days
                
                response = self.client.query_workspace(
                    workspace_id=self.workspace_id,
                    query=query,
                    timespan=(start_time, end_time)
                )
                
                if response.tables and response.tables[0].rows:
                    row_count = len(response.tables[0].rows)
                    print(f"✅ {query_name}: Found {row_count} rows")
                    working_queries.append((query_name, query))
                    
                    # Show columns for first working query
                    if len(working_queries) == 1:
                        columns = [col.name for col in response.tables[0].columns]
                        print(f"   Columns: {columns}")
                        print(f"   Sample row: {response.tables[0].rows[0][:3]}...")  # First 3 values
                else:
                    print(f"❌ {query_name}: No data")
                    
            except Exception as e:
                print(f"❌ {query_name}: Error - {e}")
        
        return working_queries
    
    async def check_cluster_specific_data(self, cluster_name=None):
        """Test 5: Check for cluster-specific data"""
        print(f"\n🔍 Test 5: Checking cluster-specific data...")
        
        if cluster_name:
            print(f"Looking for cluster: {cluster_name}")
        
        # Try to find any cluster names in the data
        discovery_queries = [
            "KubeNodeInventory | distinct ClusterName | limit 10",
            "KubePodInventory | distinct ClusterName | limit 10", 
            "Perf | where ObjectName == 'K8SNode' | distinct Computer | limit 10",
            "Heartbeat | where Category == 'Direct Agent' | distinct Computer | limit 10"
        ]
        
        for query in discovery_queries:
            try:
                end_time = datetime.now(timezone.utc)
                start_time = end_time - timedelta(days=7)
                
                response = self.client.query_workspace(
                    workspace_id=self.workspace_id,
                    query=query,
                    timespan=(start_time, end_time)
                )
                
                if response.tables and response.tables[0].rows:
                    print(f"✅ Found data with query: {query}")
                    for row in response.tables[0].rows[:5]:  # Show first 5
                        print(f"   {row}")
                    return True
                    
            except Exception as e:
                print(f"❌ Query failed: {e}")
        
        return False

async def run_comprehensive_test():
    """Run all Container Insights tests"""
    
    # Get workspace ID from environment or prompt
    workspace_id = "95eb1267-ee17-4242-b5c6-b49393c2446e"
    
    if not workspace_id:
        print("❌ AZURE_LOG_ANALYTICS_WORKSPACE_ID not found in environment")
        print("Please set it in your .env file or environment variables")
        print("\nExpected format:")
        print("/subscriptions/abc-123/resourcegroups/my-rg/providers/microsoft.operationalinsights/workspaces/my-workspace")
        print("OR just the workspace GUID: 12345678-1234-1234-1234-123456789012")
        return
    
    print(f"🚀 Testing Container Insights with workspace: {workspace_id[:50]}...")
    
    validator = ContainerInsightsValidator(workspace_id)
    
    # Run all tests
    connectivity = await validator.test_basic_connectivity()
    if not connectivity:
        print("\n❌ Basic connectivity failed. Check workspace ID and permissions.")
        return
    
    available_tables = await validator.check_available_tables()
    if not available_tables:
        print("\n❌ No Container Insights tables found. Container Insights may not be enabled.")
        return
    
    print(f"\n✅ Found {len(available_tables)} Container Insights tables")
    
    time_range_success = await validator.test_time_ranges()
    working_queries = await validator.test_specific_queries()
    cluster_data = await validator.check_cluster_specific_data()
    
    # Summary
    print("\n" + "="*60)
    print("📊 CONTAINER INSIGHTS TEST SUMMARY")
    print("="*60)
    print(f"✅ Workspace connectivity: {'SUCCESS' if connectivity else 'FAILED'}")
    print(f"✅ Available tables: {len(available_tables)}")
    print(f"✅ Data in time ranges: {'SUCCESS' if time_range_success else 'FAILED'}")
    print(f"✅ Working queries: {len(working_queries)}")
    print(f"✅ Cluster data found: {'SUCCESS' if cluster_data else 'FAILED'}")
    
    if working_queries:
        print(f"\n🎯 WORKING QUERIES FOR YOUR WORKSPACE:")
        for name, query in working_queries[:3]:  # Show top 3
            print(f"   • {name}: {query}")
    
    if not any([time_range_success, working_queries, cluster_data]):
        print(f"\n❌ NO DATA FOUND - Possible issues:")
        print(f"   1. Container Insights not enabled on AKS cluster")
        print(f"   2. Data hasn't started flowing yet (takes 5-15 minutes)")
        print(f"   3. AKS cluster doesn't exist or isn't connected to this workspace")
        print(f"   4. Wrong workspace ID")
        
        print(f"\n🔧 TROUBLESHOOTING STEPS:")
        print(f"   1. Enable Container Insights:")
        print(f"      az aks enable-addons -a monitoring -n <cluster-name> -g <resource-group> --workspace-resource-id {workspace_id}")
        print(f"   2. Wait 10-15 minutes for data to flow")
        print(f"   3. Check Azure Portal: AKS cluster -> Monitoring -> Insights")

# Quick test function for immediate use
async def quick_test():
    """Quick test with most likely working query"""
    workspace_id = "95eb1267-ee17-4242-b5c6-b49393c2446e"
    
    if not workspace_id:
        print("Set AZURE_LOG_ANALYTICS_WORKSPACE_ID in your environment")
        return
    
    client = LogsQueryClient(DefaultAzureCredential())
    
    # Try the most generic query that should work if ANY data exists
    queries_to_try = [
        ("Usage data", "Usage | limit 1"),
        ("Heartbeat", "Heartbeat | limit 1"), 
        ("Any table", "search * | limit 1"),
        ("Container Insights", "union Kube*, Perf, ContainerLog | limit 1")
    ]
    
    for name, query in queries_to_try:
        try:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=7)
            
            response = client.query_workspace(
                workspace_id=workspace_id,
                query=query,
                timespan=(start_time, end_time)
            )
            
            if response.tables and response.tables[0].rows:
                print(f"✅ {name}: SUCCESS - {len(response.tables[0].rows)} rows")
                print(f"   Sample: {response.tables[0].rows[0]}")
                return True
            else:
                print(f"❌ {name}: No data")
                
        except Exception as e:
            print(f"❌ {name}: Error - {e}")
    
    print("❌ No data found in any queries")
    return False

if __name__ == "__main__":
    print("Container Insights Testing")
    print("=" * 40)
    print("1. Quick test")
    print("2. Comprehensive test")
    
    choice = input("\nEnter choice (1 or 2): ").strip()
    
    if choice == "1":
        asyncio.run(quick_test())
    elif choice == "2":
        asyncio.run(run_comprehensive_test())
    else:
        print("Invalid choice")