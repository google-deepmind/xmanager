"""XManager launch API implementation for XManager on Cloud."""

from xmanager.xm_cloud import executor
from xmanager.xm_cloud import experiment

from xmanager_cloud.experiment_state_server.proto import priority_pb2
from xmanager_cloud.experiment_state_server.proto import structured_message_pb2
from xmanager_cloud.experiment_state_server.proto import url_pb2
from xmanager_cloud.experiment_state_server.proto import work_unit_pb2
from xmanager_cloud.xid_service.proto import messages_pb2 as xid_messages_pb2

XManagerCloudExperiment = experiment.XManagerCloudExperiment
XManagerCloudWorkUnit = experiment.XManagerCloudWorkUnit
WorkUnitSettings = experiment.WorkUnitSettings

KubernetesJobExecutor = executor.KubernetesJobExecutor
KubernetesJobExecutorSpec = executor.KubernetesJobExecutorSpec

create_experiment = experiment.create_experiment
get_experiment = experiment.get_experiment
get_work_unit = experiment.get_work_unit
list_experiments = experiment.list_experiments
delete_artifact = experiment.delete_artifact
get_current_experiment = experiment.get_current_experiment
get_current_work_unit = experiment.get_current_work_unit
update_artifact = experiment.update_artifact

Priority = priority_pb2.Priority
Url = url_pb2.Url
Acls = xid_messages_pb2.XidAcls
SchedulingConstraints = work_unit_pb2.SchedulingConstraints
Artifact = experiment.Artifact
StructuredMessage = structured_message_pb2.StructuredMessage
