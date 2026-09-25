"""Main entrypoint for Vault API Gateway."""

import os
from pathlib import Path
import uvicorn
from apps.gateway.service import GatewayService, create_gateway_app
from vault_core.cluster import ClusterConfig
from vault_core.quorum import load_policies_from_yaml
from vault_core.settings import settings

cluster_config_path = os.getenv("VAULT_CLUSTER_CONFIG_PATH", "config/cluster.yaml")
policies_config_path = os.getenv("VAULT_POLICIES_CONFIG_PATH", "config/policies.yaml")

cluster = ClusterConfig.from_yaml_file(cluster_config_path)
policies = load_policies_from_yaml(policies_config_path)

gateway_service = GatewayService(
    cluster=cluster,
    policies=policies,
    custom_settings=settings,
)

app = create_gateway_app(gateway_service)

if __name__ == "__main__":
    uvicorn.run(
        "apps.gateway.main:app",
        host="0.0.0.0",
        port=int(os.getenv("VAULT_PORT", "8000")),
        reload=False,
    )
