# XManager Cloud (XMC)

XManager Cloud (XMC) is the client SDK for XManager on GCP/GKE. It manages experiment state and interacts with cloud-based backend services.

## Backend Services
The backend services for XMC are located in the [google/xmc](https://github.com/google/xmc) repository.

## Installation and Setup
Run the setup script from the repository root to create a virtual environment, install python dependencies, and compile the protobuf files:

```bash
bash setup_scripts/install_xmanager_cloud.sh
```

The script performs the following steps:
1. Creates a Python virtual environment (`venv/`) at the repository root.
2. Installs required python packages (`grpcio`, `grpcio-tools`, `google-auth`, `googleapis-common-protos`, `requests`).
3. Clones the `googleapis` protobuf definition repository to `/tmp/googleapis`.
4. Compiles the gRPC proto files under `xmanager_cloud/`.
5. Verifies imports for `xmanager_cloud`.

## Authentication
`experiment_state_api` resolves client credentials in the following order:
1. **`XMC_AUTH_TOKEN`**: Explicit Bearer token override from the environment.
2. **Cached CLI Credentials (`xmc login`)**: Per-user CLI session token cached in `$XDG_CONFIG_HOME/xmc/credentials.json` (defaulting to `~/.config/xmc/credentials.json`) matching the deployment origin (`XMANAGER_ENDPOINT`, or `XMC_API_URL` / `XMANAGER_UI_URL` for local port-forwards).
3. **Service Account Impersonation (`XMC_CLIENT_SA` + `IAP_CLIENT_ID`)**: Google Cloud IAM OIDC token generation for non-interactive CI and service account workloads.
