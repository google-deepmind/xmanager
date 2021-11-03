# Executable specifications

## Container

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

## BazelContainer

BazelContainer defines a Bazel target identified by a label producing a
container image. [rules_docker](https://github.com/bazelbuild/rules_docker)
provides appropriate rules such as `container_image`. Note that for a target
with `name='image'` one should use the target `image.tar` to point XManager
to the self-contained `.tar` it expects.

```python
xm.BazelContainer(label='//path/to/target:label.tar')
```

`xm.bazel_container` is a shortener for packageable construction.

```python
assert xm.bazel_container(
    executor_spec=xm_local.Local.Spec(),
    args=args,
    env_vars=env_vars,
    ...
) == xm.Packageable(
    executable_spec=xm.BazelContainer(...),
    executor_spec=xm_local.Local.Spec(),
    args=args,
    env_vars=env_vars,
)
```

## Binary

Binary defines a pre-built binary located locally.

```python
xm.Binary(path='/home/user/project/a.out')
```

`xm.binary` is a shortener for packageable construction.

```python
assert xm.binary(
    executor_spec=xm_local.Local.Spec(),
    args=args,
    env_vars=env_vars,
    ...
) == xm.Packageable(
    executable_spec=xm.Binary(...),
    executor_spec=xm_local.Local.Spec(),
    args=args,
    env_vars=env_vars,
)
```

## BazelBinary

BazelBinary defines a Bazel binary target identified by a label. To build
self-contained Python binaries use [subpar](https://github.com/google/subpar).

```python
xm.BazelBinary(label='//path/to/target:label')
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

## PythonContainer

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
