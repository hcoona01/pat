"""Public Cloud Run demo; deliberately not a distributed storage data plane."""
import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title="Vault Demo", version=os.getenv("VAULT_VERSION", "0.1.0"))

NOTICE = (
    "This Cloud Run service is a public Vault demo and documentation endpoint. "
    "The verified multi-node fault-tolerance demonstration runs through Docker Compose "
    "E2E tests; this Cloud Run instance is not a six-node storage cluster."
)

@app.get("/", response_class=HTMLResponse)
async def landing() -> str:
    return f"""<!doctype html><html><head><title>Vault Demo</title></head><body>
    <h1>Vault</h1><p>{NOTICE}</p>
    <ul><li><a href='/healthz'>Health</a></li><li><a href='/architecture'>Architecture</a></li>
    <li><a href='/limitations'>Limitations</a></li><li><a href='/docs'>API docs</a></li></ul></body></html>"""

@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok", "mode": "cloud_demo", "version": app.version, "git_sha": os.getenv("GIT_SHA", "unknown")}

@app.get("/architecture")
async def architecture() -> dict:
    return {"components": ["API gateway", "three-node Raft metadata cluster", "six storage nodes", "repair and rebalance workers"], "local_verification": "Docker Compose E2E"}

@app.get("/limitations")
async def limitations() -> dict:
    return {"notice": NOTICE, "cloud_run": "stateless demo only; no local persistent distributed storage is used"}
