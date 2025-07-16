# Parameter Controller

XManager supports running multiple executables across your work units. This
feature is most relevant for when you have multiple executables you want to run
with XManager, but you want to wait for one executable to finish before
launching the next one. These work units may have a different number of tasks or
different resource requirements.

Some valid use cases for using pipelines include:

-   Training followed by model evaluation.
-   Data extraction/generation/transformation followed by training.
-   Running training work units across different architectures
    (CPU/P100/V100/TPU).

XManager does not provide an API for transferring data across work units,
however this can be done by using shared cloud storage solutions like GCS.

Using a parameter controller, you can specify a customized executable ordering.
You can launch a linear multi-executable workflow as in the example below:

```py
# Launch preprocess and wait until it is complete.
preprocess = await experiment.add(preprocess_job)
await preprocess.wait_until_complete()

# Launch training on multiple datasets in parallel.
train_operations = [
    await experiment.add(train_job)
    for dataset in ['cifar10', 'mnist']]
await asyncio.gather(
    *(op.wait_until_complete() for op in train_operations))

# When they are finished, evaluate the result.
experiment.add(evaluate_job)
```

To run this in a dedicated remote auxiliary unit, place it in a function
decorated with the `@parameter_controller.controller` decorator. Permissions or
configuration details can be set in the parameter controller job similarly to
normal jobs, through the `controller_args` and `controller_env_vars` variables
or files in the `package_directory` specified.

```py
@parameter_controller.controller(
    executor=xm_local.Kubernetes(),
    controller_args={
        'xm_k8s_service_account_name':
            flags.FLAGS.xm_k8s_service_account_name,
        'xm_gcp_service_account_name':
            flags.FLAGS.xm_gcp_service_account_name,
    },
    controller_env_vars={
        'GOOGLE_CLOUD_BUCKET_NAME': os.environ['GOOGLE_CLOUD_BUCKET_NAME'],
    },
    use_host_db_config=True,
    # Package contents of this directory inside parameter controller job
    package_path='.')
async def run_pipeline(experiment: xm.Experiment):
  # Launch preprocess and wait until it is complete.
  preprocess = await experiment.add(preprocess_job)
  await preprocess.wait_until_complete()

  # Launch training on multiple datasets in parallel.
  train_operations = [
      await experiment.add(train_job)
      for dataset in ['cifar10', 'mnist']]
  await asyncio.gather(
      *(op.wait_until_complete() for op in train_operations))

  # When they are finished, evaluate the result.
  experiment.add(evaluate_job)

experiment.add(run_pipeline())
```

An example of a launch script using a parameter controller job running on
Kubernetes to launch other jobs on Vertex and Kubernetes is provided at
[`examples/parameter_controller/launcher.py`](https://github.com/deepmind/xmanager/blob/main/examples/parameter_controller/launcher.py)

NOTE: A parameter controller job running on VertexAI can't currently launch jobs
on Kubernetes.

## Setup

The parameter controller job uses XManager to launch new jobs, so the
environment variables and flags used by XManager can also be used by the client
inside the controller job.

The following dependencies are required by the controller job and must be
provided through a `requirements.txt` file which is packaged with the job:

```
absl-py
six
sqlparse
cloudpickle
```

If using the CloudSql connector, `cloud-sql-python-connector[${DB_BACKEND}]`
may also be required.

## Remote Database

Since the parameter controller needs to observe and update the context of the
experiment, it needs access to the database containing the data. Therefore, a
remote database that can be accessed from remote jobs must be used by both the
host launching the controller job and the controller job itself.

This is done by using the `use_host_db_config` parameter in the controller job
and using the `--xm_db_yaml_config_path` flag in the master script. For more
information on how remote databases can be used, see the
[guide on metadata storage](https://github.com/deepmind/xmanager/blob/main/docs/metadata_storage.md).

## Required Permissions

Since the parameter controller needs to package executables and launch jobs, it
needs special permissions. The GCP service account used to launch jobs must have
the following IAM roles:

-   **Storage Admin**: GCS read/write
-   **Cloud Build Editor**: package jobs locally/in remote controller job
-   **Vertex AI User** (optional): launch Vertex AI jobs
-   **CloudSQL Editor** (optional): access to CloudSQL databases

If running on Kubernetes, it's assumed that the job inside the cluster already
has the required permissions. Specifically for GKE, this can be done by using
[Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#authenticating_to)
to map the GCP service account to a Kubernetes Service Account. Using custom
service accounts can be done through the `--xm_k8s_service_account_name` and
`--xm_gcp_service_account_name` flags. Note that the default GCP service account
is named `xmanager` and the default KSA is `default`.

Additionally, the used Kubernetes service account must have the following RBAC
permissions to allow the controller job to launch and track new jobs:

-   **List services**
-   **Create batch jobs**
-   **Get status of batch jobs**

## Slides

Remote metadata execution and metadata storage are present in
[these slides](https://storage.googleapis.com/gresearch/xmanager/remote_execution_slides.pdf).
