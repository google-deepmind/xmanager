---
name: xmanager_cloud
description: >-
    Launches, manages, and troubleshoots XManager Cloud (XMC) experiments. A MUST READ before launching any XMC experiment, orchestrating jobs on Kubernetes/GKE, or configuring experiment flags. Don't use for internal corporate launches.
---

<!-- disableFinding(LINE_OVER_80) -->

> [!IMPORTANT]
> **GOOGLE INTERNAL AGENT NOTICE:** If you are an automated agent operating inside a Google internal workspace, **DO NOT USE THIS SKILL**. This instruction is strictly adapted for external open-source Kubernetes and XMC environments. For Google internal launches, please switch to the internal version of the XManager skill.

# Launching and Managing XManager Cloud (XMC) Experiments

XManager Cloud (XMC) is an open-source platform for machine learning experimentation. It enables ML researchers and engineers to programmatically package, launch, manage, and analyze multi-stage ML experiments across Google Kubernetes Engine (GKE) and external compute clusters.

**Recommended pre-read:** Familiarity with Python container orchestration, Docker, and Kubernetes/Kueue resource management.

## 1. CLI & SDK Installation

Install the official XManager Python SDK and CLI tools directly via pip:

```bash
pip install xmanager
```

To verify your installation and check available commands:

```bash
xmc help
```

If you are developing or modifying the XManager core library, install it locally in editable mode from your cloned git repository:

```bash
git clone https://github.com/google/xmanager.git
cd xmanager
pip install -e .
```

## 2. Referring to Experiments

Every successful experiment launch is assigned a unique Experiment ID (**XID**). Always track and use your XID (e.g., `12345`) when querying state, viewing dashboards, or referencing experiments in documentation and issue trackers.

## 3. Selecting a Resource Queue

Before launching an experiment, verify your available Kubernetes/Kueue resource allocations (`ClusterQueues` or `LocalQueues`) to ensure you have sufficient compute capacity (e.g., GPUs, TPUs, or high-memory CPUs).

```bash
# Query available resource queues and remaining quotas on your active cluster
kubectl get clusterqueues
kubectl get localqueues -n <your-namespace>
```

Resource allocations are typically passed to your executable specifications or via flags (e.g., `--queue_name=gpu-research-queue`).

## 4. Launching an Experiment

Experiments are declaratively defined using Python orchestration scripts that specify container packaging, hyperparameters, work units, and execution topologies.

Launch an experiment using the official CLI:

```bash
xmc launch path/to/launcher.py -- --experiment_title="ResNet50 Scaling" --learning_rate=0.001
```

**Note:** Any custom application flags intended for your experiment workload should follow the `--` separator.

### Background Executions

Because building container images and registering complex multi-node experiments can take time, run large launches inside persistent terminal sessions (e.g., `tmux` or `screen`) so they continue running safely if your connection drops.

## 5. Inspecting Experiment Details & Listing (Web UI & CLI)

XManager Cloud provides a feature-rich, dedicated graphical **Web UI** for exploring experiment hierarchies, hyperparameter sweeps, active work units, and runtime configurations.

**External users are highly encouraged to deploy the official XMC Web UI:** Visit the official repository at [https://github.com/google/xmc](https://github.com/google/xmc) for step-by-step deployment instructions using Helm and Terraform.

### Suggestion for Agents & Users

When inspecting complex experiments, prioritize using the **XMC Web UI** over raw CLI outputs. The UI provides a significantly cleaner interface for:
- Visualizing hierarchical work units and auxiliary sidecar jobs.
- Filtering hyperparameter sweeps and runtime dictionary configurations.
- Direct one-click access to attached evaluation artifacts (TensorBoard, model checkpoints).

### Listing Active Experiments via CLI

If terminal verification is required in automation scripts, list your active or recent experiments matching an AIP-160 filter query:

```bash
# List all recent experiments
xmc list

# Filter experiments by title or status
xmc list --filter_query="title = \"ResNet50 Scaling\""
```

## 6. Accessing Logs & Artifacts

### Live Pod Logging via Kubectl

Because the open-source CLI delegates runtime execution directly to Kubernetes, stream your real-time application and training logs using standard `kubectl` with the official XMC pod label (`xmc.google.com/experiment-id`):

```bash
kubectl logs -f -l xmc.google.com/experiment-id=<XID> -n <your-namespace>
```

### Metrics & Visualization

Access your experiment via the **XMC Web UI** to locate attached TensorBoard links, metrics summaries, or Cloud Storage checkpoint directories.

## 7. Restarting & Stopping Experiments

To cleanly manage active workloads or restart failed executions:

```bash
# Restart all work units in an experiment by XID
xmc restart 12345

# Stop and cancel an active experiment by XID
xmc stop 12345
```

## 8. Troubleshooting Common Issues

When diagnosing a failing or unresponsive experiment, follow this sequential debugging hierarchy:

1. **Check Work Unit Status Messages:** Access your experiment via the **XMC Web UI** or run `xmc list`. Prioritize individual **Work Unit status messages** over the top-level experiment status.
2. **Container Packaging / ModuleNotFoundError:** If work units fail immediately with module import errors, verify your Dockerfile build context or Python dependency requirements (`requirements.txt`). Ensure all necessary packages are copied correctly during container build.
3. **Work Unit Stuck in `PREPARING` / `PENDING`:**
    - **Queue Quota Exhausted:** If your jobs remain pending for extended periods, check if your target Kueue `ClusterQueue` has exhausted its compute quota:
      ```bash
      kubectl describe clusterqueue <queue-name>
      ```
    - **Unsatisfiable Scheduling Constraints:** Verify that your requested resources (e.g., specific GPU accelerators or memory limits) match actual available node pools on your Kubernetes cluster.
4. **Inspect Failed Pod Lifecycle Events:** If a Kubernetes job crashed or was evicted, inspect the raw pod lifecycle events:
    ```bash
    kubectl describe pods -l xmc.google.com/experiment-id=<XID>
    ```
