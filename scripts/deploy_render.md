# Free public demo deployment (Render)

This deploys the `apps.cloud_demo` FastAPI interface, not the stateful multi-node
cluster. It is intentionally labelled as a demo because Render Free provides one
ephemeral container, while Vault needs independent nodes and persistent volumes.

1. Push this repository to a public GitHub repository.
2. Sign in at https://dashboard.render.com using GitHub and choose the **Free**
   web-service plan. Do not enter payment details; if the provider requests
   them, stop and use a different hosting account.
3. Choose **New** -> **Blueprint**, select the repository, and deploy the
   `render.yaml` file.
4. In the environment-variable prompt, set `GIT_SHA` to the commit being shown.
5. After the health check passes, copy the generated `https://...onrender.com`
   URL. Verify `/`, `/healthz`, `/architecture`, and `/limitations` before
   placing it in the submission form.

The Free service may sleep after idle time and has an ephemeral filesystem; it
must not be presented as the distributed durability deployment. The repository
and Docker E2E evidence are the proof for the full topology.
