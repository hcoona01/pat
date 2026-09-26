# Cloud Run demo deployment

The Cloud Run service is a public documentation and health demo only. It is not the
six-node Vault cluster and must not be presented as proof of distributed durability.

For a no-card public presentation link, use the Render Free Blueprint described in
`scripts/deploy_render.md`. Cloud Run requires a billed Google Cloud project, so it
is not the no-billing route.

After authenticating `gcloud`, run:

```bash
export GOOGLE_CLOUD_PROJECT=your-project-id
export GOOGLE_CLOUD_REGION=asia-south1
./scripts/deploy_cloud_run.sh
```

The command prints the only deployment URL that may be submitted. Record it only after
the command succeeds and verify `/healthz` returns `mode: cloud_demo`.
