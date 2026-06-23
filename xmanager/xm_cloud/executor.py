"""Executors for different types of resources and execution platforms.

Only supports Kubernetes jobs for now.
"""

import enum

import attr
from xmanager import xm


class JobDependencyState(enum.Enum):
  """State that a dependent job must be in for the main job to start."""

  # The dependent job must be ready to start the main job. This is useful for
  # server-client dependencies where the client job should not start until
  # the server job is ready.
  #
  # See
  # https://jobset.sigs.k8s.io/docs/reference/jobset.v1alpha2/#jobset-x-k8s-io-v1alpha2-ReplicatedJobStatus
  # for more details.
  READY = 'READY'
  # The dependent job must be completed successfully for the main job to start.
  # This is useful for sequential dependencies where the next job should not
  # start until the dependent job is completed successfully.
  COMPLETED = 'COMPLETED'


@attr.s(auto_attribs=True)
class Dependency:
  """Dependency on another job.

  Attributes:
    job_name: The name of the job that this job depends on. This must be a job
      previously added to the same work unit. Jobs cannot depend on jobs in
      other work units, or jobs that are added later to the same work unit.
    state: The state that the dependent job must be in.
  """

  job_name: str
  state: JobDependencyState = JobDependencyState.READY


@attr.s(auto_attribs=True)
class KubernetesJobExecutorSpec(xm.ExecutorSpec):
  # The container registry to push packageable images to.
  container_registry: str | None = None
  # The name of the image to push.
  image_name: str | None = None
  # The tag to append to the image. Note the image path will then be in the form
  # '{container_registry}/{image_name}:{image_tag}'
  image_tag: str | None = None


@attr.s(auto_attribs=True)
class KubernetesJobExecutor(xm.Executor):
  """Executor for jobs running on Kubernetes.

  Attributes:
    requirements: Resources the job needs.
    replica_retry_limit: Limit the total number of retries to tolerate before
      considering the job failed, across all replicas. Defaults to num_replicas
      * 5
    active_deadline_seconds: The active deadline of the Kubernetes Job. This is
      the maximum amount of time the Kubernetes Job can run before it is killed.
      Note this is not a deadline per execution, this is a deadline for the
      entire Kubernetes Job to be active. If a pod fails and is restarted, the
      deadline is not reset.
    annotations: Optional key-value string pairs that can be passed to
      downstream systems.
  """

  requirements: xm.JobRequirements = attr.Factory(xm.JobRequirements)
  replica_retry_limit: int | None = attr.ib(kw_only=True, default=None)
  active_deadline_seconds: int | None = attr.ib(kw_only=True, default=None)
  annotations: dict[str, str] | None = attr.ib(kw_only=True, default=None)
  dependency: Dependency | None = attr.ib(kw_only=True, default=None)
  Spec = KubernetesJobExecutorSpec  # pylint: disable=invalid-name
