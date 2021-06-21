# XManager: A framework for managing experiments

## Prerequisites

### Install Docker

If you use `xmanager.xm.PythonDocker` to run XManager experiments,
you need to install Docker.

1. Follow the steps to install Docker here: https://docs.docker.com/engine/install/#supported-platforms

2. And if you are a Linux user, follow the steps to enable sudoless Docker here: https://docs.docker.com/engine/install/linux-postinstall/

### Install Bazel

If you use `xmanager.xm_local.BazelContainer` or `xmanager.xm_local.BazelBinary`
to run XManager experiments, you need to install Bazel.

1. Follow the steps to install Bazel here: https://docs.bazel.build/versions/master/install.html

### Create a Google Cloud Platform (GCP) project

If you use `xm_local.Caip` to run XManager experiments, you need
to have a GCP project in order to be able to access Cloud AI Platform to run
jobs.

1. Create a GCP project here: https://console.cloud.google.com/

2. Install `gcloud` here: https://cloud.google.com/sdk/docs/install

3. Associate your Google Account (Gmail account) with your GCP project by
   running:

   ```bash
   export GCP_PROJECT=<GCP PROJECT ID>
   gcloud auth login
   gcloud auth application-default login
   gcloud config set project $GCP_PROJECT
   ```

4. Set up `gcloud` to work with Docker by running:

   ```bash
   gcloud auth configure-docker
   ```

5. Enable Google Cloud Platform APIs.

   * Enable IAM here: https://console.cloud.google.com/apis/library/iam.googleapis.com

   * Enable the 'Cloud AI Platfrom' here: https://console.cloud.google.com/apis/library/aiplatform.googleapis.com

   * Enable the 'Container Registry' here: https://console.cloud.google.com/apis/library/containerregistry.googleapis.com

6. Create a staging bucket in us-central1 if you do not already have one. This
   bucket should be used to save experiment artifacts like Tensorflow log files,
   which can be read by Tensorboard. This bucket may also be used to stage files
   to build your Docker image if you build your images remotely.

   ```bash
   export GOOGLE_CLOUD_BUCKET_NAME=<GOOGLE_CLOUD_BUCKET_NAME>
   gsutil mb -l us-central1 gs://$GOOGLE_CLOUD_BUCKET_NAME
   ```

   Add the GOOGLE_CLOUD_BUCKET_NAME to your environment variables or your .bashrc:

   ```bash
   export GOOGLE_CLOUD_BUCKET_NAME=<GOOGLE_CLOUD_BUCKET_NAME>
   ```

## Install XManager

```bash
pip install ./xmanager
```

## Run XManager

Run a launch script, e.g.

```bash
xmanager launch ./xmanager/examples/cifar10/launcher.py
```

## Writing XM launch scripts

The basic structure of an XManager launch script can be summarized by these
steps:

1. Define your experiment context.

    ```python
    from xmanager import xm
    from xmanager import xm_local

    with xm_local.create_experiment(experiment_title='cifar10') as experiment:
    ```

2. Define specifications of executables you want to run.

    ```python
    spec = xm.PythonContainer(
        path='/path/to/python/folder',
        entrypoint=xm.ModuleName('cifar10'),
    )
    ```

3. Package your executables.

    ```python
    from xmanager import xm_local

    [executable] = experiment.package([
      xm.Packageable(
        executable_spec=spec,
        executor_spec=xm_local.Caip.Spec(),
      ),
    ])
   ```

4. Define your hyperparameters.

    ```python
    import itertools

    batch_sizes = [64, 1024]
    learning_rates = [0.1, 0.001]
    trials = list(
      dict([('batch_size', bs), ('learning_rate', lr)])
      for (bs, lr) in itertools.product(batch_sizes, learning_rates)
    )
    ```

5. Define resources each job should consume.

    ```python
    resources = xm.JobRequirements(T4=1)
    ```

6. For each trial, add a job / job groups to launch them.

    ```python
    for hyperparameters in trials:
      experiment.add(xm.Job(
          executable=executable,
          executor=xm_local.Caip(resources=resources),
          args=hyperparameters,
        ))
    ```

## Components

### Executable specifications

XManager executable specifications define what should be packaged in the form of
binaries, source files, and other input dependencies required for job execution.
Executable specifications are reusable are generally platform-independent.

#### Container

Container defines a pre-built Docker image located at a URL (or locally).

```python
xm.Container(path='gcr.io/project-name/image-name:latest')
```

`xm.container` is a shortener for packageable construction.

```python
assert xm.container(
    executor_spec=xm_local.Local.Spec(),
    args=args,
    env_vars=env_vars,
    ...
) == xm.Packageable(
    executable_spec=xm.Container(...),
    executor_spec=xm_local.Local.Spec(),
    args=args,
    env_vars=env_vars,
)
```

#### BazelBinary

BazelBinary defines a Bazel binary target identified by a label.

```python
xm.Binary(path='//path/to/target:label')
```

`xm.bazel_binary` is a shortener for packageable construction.

```python
assert xm.bazel_binary(
    executor_spec=xm_local.Local.Spec(),
    args=args,
    env_vars=env_vars,
    ...
) == xm.Packageable(
    executable_spec=xm.BazelBinary(...),
    executor_spec=xm_local.Local.Spec(),
    args=args,
    env_vars=env_vars,
)
```

#### PythonContainer

PythonContainer defines a Python project that is packaged into a Docker
container.

```python
xm.PythonContainer(
    entrypoint: xm.ModuleName('<module name>'),

    # Optionals.
    path: '/path/to/python/project/',  # Defaults to the current directory of the launch script.
    base_image: '<image>[:<tag>]',
    docker_instructions: ['RUN ...', 'COPY ...', ...],
)
```

A simple form of PythonContainer is to just launch a Python module with default
`docker_intructions`.

```python
xm.PythonContainer(entrypoint=xm.ModuleName('cifar10'))
```

That specification produces a Docker image that runs the following command:

```
python3 -m cifar10 fixed_arg1 fixed_arg2
```

An advanced form of PythonContainer allows you to override the entrypoint
command as well as the Docker instructions.

```python
xm.PythonContainer(
    entrypoint=xm.CommandList([
      './pre_process.sh',
      'python3 -m cifar10 $@',
      './post_process.sh',
    ]),
    docker_instructions=[
      'COPY pre_process.sh pre_process.sh',
      'RUN chmod +x ./pre_process.sh',
      'COPY cifar10.py',
      'COPY post_process.sh post_process.sh',
      'RUN chmod +x ./post_process.sh',
    ],
)
```

That specification produces a Docker image that runs the following commands:

```
./pre_process.sh
python3 -m cifar10 fixed_arg1 fixed_arg2
./post_process.sh
```

IMPORTANT: Note the use of `$@` which accepts command-line arguments. Otherwise,
all command-line arguments are ignored by your entrypoint.

`xm.python_container` is a shortener for packageable construction.

```python
assert xm.python_container(
    executor_spec=xm_local.Local.Spec(),
    args=args,
    env_vars=env_vars,
    ...
) == xm.Packageable(
    executable_spec=xm.PythonContainer(...),
    executor_spec=xm_local.Local.Spec(),
    args=args,
    env_vars=env_vars,
)
```

### Executors

XManager executors define the platform where the job runs and the resources that
the job consumes.

Each executor also has a specification which describes how an executable
specification should be prepared and packaged.

#### Cloud AI Platform (CAIP)

The Caip executor declares that an executable will be run on the CAIP platform.

The Caip executor takes in a resource object.

```python
xm_local.Caip(
    xm.JobRequirements(
        cpu=1,  # Measured in vCPUs.
        ram=4 * xm.GiB,
        T4=1,  # NVIDIA Tesla T4.
    ),
)
```

```python
xm_local.Caip(
    xm.JobRequirements(
        cpu=1,  # Measured in vCPUs.
        ram=4 * xm.GiB,
        TPU_V2=8,  # TPU v2.
    ),
)
```

As of June 2021, the valid accelerator types are:

* `P100`
* `V100`
* `P4`
* `T4`
* `A100`
* `TPU_V2`
* `TPU_V3`

IMPORTANT: Note that for `TPU_V2` and `TPU_V3` the only valid count is currently
8.

##### Caip Specification

The CAIP executor allows you specify a remote image repository to push to.

```python
xm_local.Caip.Spec(
    push_image_tag='gcr.io/<project>/<image>:<tag>',
)
```

#### Local

The local executor declares that an executable will be run on the same machine
from which the launch script is invoked.

#### Kubernetes (experimental)

The Kubernetes executor declares that an executable will be run on a Kubernetes
cluster. As of March 2021, Kubernetes is not fully supported.

The Kubernetes executor pulls from your local `kubeconfig`. The XManager
command-line has helpers to set up a Google Kubernetes Engine (GKE) cluster.

```bash
pip install caliban==0.4.1
xmanager cluster create

# cleanup
xmanager cluster delete
```

You can store the GKE credentials in your `kubeconfig`:

```bash
gcloud container clusters get-credentials <cluster-name>
```

##### Kubernetes Specification

The Kubernetes executor allows you specify a remote image repository to push to.

```python
xm_local.Kubernetes.Spec(
    push_image_tag='gcr.io/<project>/<image>:<tag>',
)
```

### Job / JobGroup

Each Job and JobGroup objects defines a specific run and can not be run multiple
times. While a job defines a single run of a single executable, a JobGroup
defines a single run of nested Jobs containing many executables of the same type
or different types. JobGroups provide the gang scheduling concept: Jobs inside
them are scheduled / descheduled simultaneously.

#### Job

A Job accepts an executable and an executor along with hyperparameters which can
either be command-line arguments or environment variables.

Command-line arguements can be passed in list form, `[arg1, arg2, arg3]`:

```bash
binary arg1 arg2 arg3
```

They can also be passed in dictionary form, `{key1: value1, key2: value2}`:

```bash
binary --key1=value1 --key2=value2
```

Environment variables are always passed in `Dict[str, str]` form:

```bash
export KEY=VALUE
```

Jobs are defined like this:

```python
[executable] = xm.Package(...)

executor = xm_local.Caip(...)

xm.Job(
    executable=executable,
    executor=executor,
    args={
        'batch_size': 64,
    },
    env_vars={
        'NCCL_DEBUG': 'INFO',
    },
)
```

#### JobGroup

A JobGroup accepts jobs in a keyword form. The keyword can be any valid Python
keyword. For example, you can call your jobs 'agent' and 'observer'.

```python
agent_job = xm.Job(...)
observer_job = xm.Job(...)

xm.JobGroup(agent=agent_job, observer=observer_job)
```
