# XManager CLI

This directory contains the command-line interface for XManager.

Currently, there's only one command for XManager which is the launch command:

```
xmanager launch path/to/launch/script.py
```

## GKE

In order to use the Kubernetes executor, you must host a Kubernetes cluster or
use a Kubernetes cluster hosted by a cloud provider. An easy way to quickly
create a cluster is by using [caliban](https://caliban.readthedocs.io/).

The `xmanager` CLI can create clusters by calling caliban. To create a GKE
auto-scaling cluster to be used with the Kubernetes executor, run:

```
xmanager cluster create
```

This command is equivalent to running `caliban cluster create`.

To delete this cluster:

```
xmanager cluster create
```

This command is equivalent to running `caliban cluster delete`.

<!-- TODO: Implement `list`, `stop`, etc. -->
