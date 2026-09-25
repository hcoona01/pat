"""Main entrypoint for storage node application."""

import uvicorn
from apps.storage_node.service import app
from vault_core.settings import settings

if __name__ == "__main__":
    uvicorn.run(
        "apps.storage_node.main:app",
        host=settings.host,
        port=settings.port,
        reload=False,
    )
