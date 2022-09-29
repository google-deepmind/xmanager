# TensorBoard

Local experiments can be visualized using TensorBoard. This is done by making
use of the `contrib.tensorboard` package or the native support of the
`xm_local.Vertex` executor.

For `xm_local.Vertex`, one can pass use the `xm_local.TensorboardCapability`
spec to give details about the TensorBoard instance, while the
`contrib.tensorboard.add_tensorboard` function can be used for `xm_local.Local`
and `xm_local.Kubernetes`.

## Log Sharing

The most important detail given to the TensorBoard job is the path to the
directory where logs are stored. In experiments using `xm_local.Local`, this
path can be a directory available locally. Most of the times, this path must be
accessible remotely from all the workers and the TensorBoard job.

The recommended workflow supported by all back-ends is using Cloud Storage
Storage URLs paths for this. They are of the form
`gs://${GOOGLE_BUCKET_NAME}/${TARGET_PATH}` and there is native support for them
in TensorFlow, PyTorch and TensorBoard.

[gcsfuse](https://cloud.google.com/storage/docs/gcs-fuse) paths mounted inside
jobs can also be used for this, but support for them is not available for
Kubernetes.

Both approaches above require that the required GCP permission are granted to
the jobs. In general, training jobs that read/create/delete GCS logs need to be
associated with a service account having the Storage Objects Admin IAM role,
while Tensorboard only needs the Storage Objects Viewer IAM role. For more
information, check
[IAM roles for Cloud Storage](https://cloud.google.com/storage/docs/access-control/iam-roles)
for more information on permissions.

## Examples

### Local Docker container

Tensorboard can be deployed as an auxiliary unit running locally through the
`xm_local.Local` executor.

Visualizations can be made visible to the host by making the port used by
Tensorboard visible outside the container. This way, the user can open a browser
at `localhost:6006` and see the visualizations.

If using `gs://` paths for storing logs, one must also mount the required GCP
credentials in the container. Make sure that the user credentials are
available at `~/.config/gcloud` by running `gcloud auth application-default
login`.

GCSfuse paths mounted on the host can also be used for the log directory. This
is done by mounting the bucket at `~/gcs` and using `mount_gcs_path=True` in the
`xm.DockerOptions` passed to the executor.

Both of these are achieved by passing the right parameters to the executor
through `xm_local.DockerOptions`. Adding the following to a launch script will
deploy a Tensorboard container locally:

```py
tensorboard.add_tensorboard(
  experiment,
  log_dir,                          # Log directory used
  port,                             # Port used by TensorBoard inside container
  timeout_secs=timeout_secs,        # Time passed before stopping the server
  executor=xm_local.Local(
    docker_options=xm_local.DockerOptions(
      # Make visualization visible to host on `localhost:6006`.
      ports={6006:port},
      # Mount GCP credentials inside Docker so `gs://` path can be accessed.
      volumes={os.path.expanduser('~/.config/gcloud'): '/root/.config/gcloud'}
)))
```

### Vertex

`xm_local.Vertex` has native support for Tensorboard. The executor can take an
`xm_local.TensorboardCapability` spec as a parameter, which specifies the name
of the TensorBoard instance, as well as the log directory to be used.

When creating jobs, a Tensorboard instance can be created/associated with them
by doing something similar to this:

```py
tensorboard_capability = xm_local.TensorboardCapability(
  # Name of the TensorBoard instance used. Will be created if it doesn't exist.
  name=tensorboard,
  # Log directory used
  base_output_directory=output_dir)
experiment.add(
    xm.Job(
    executable=executable,
    executor=xm_local.Vertex(tensorboard=tensorboard_capability)
))
```

An example of a launch script using TensorBoard on Vertex is provided at
[`examples/cifar10_tensorflow/launcher.py`](https://github.com/deepmind/xmanager/blob/main/examples/cifar10_tensorflow/launcher.py).

### Kubernetes

Similarly to the `xm_local.Local` executor, a TensorBoard server is deployed on
a Kubernetes pod using `contrib.tensorboard.add_tensorboard`.

Visualisations can be made available to the host by forwarding the port on the
pod to one available on the host. This is done by using the `kubectl
port-forward` command as follows:

```bash
$ kubectl port-forward ${TENSORBOARD_JOB_POD}:${HOST_PORT}:${TENSORBOARD_PORT}
```

If using `gs://` paths, the job must have the required permissions. This is
usually done by mapping a GCP service account to a KSA through
[GKE Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
and then using the `--xm_k8s_service_account_name` flag to make Kubernetes jobs
use that account.

Launching a Tensorboard auxiliary unit is done by adding the following to the
launch script:

```py
tensorboard.add_tensorboard(
  experiment,
  log_dir,                          # Log directory used
  port,                             # Port used by TensorBoard inside container
  timeout_secs=timeout_secs,        # Time passed before stopping the server
  executor=xm_local.Kubernetes(),
)
```

An example of an launch script using Tensorboard on Kubernetes is available at
[`examples/cifar10_tensorflow_k8s_tensorboard/launcher.py`](https://github.com/deepmind/xmanager/blob/main/examples/cifar10_tensorflow_k8s_tensorboard/launcher.py).
