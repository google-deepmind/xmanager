# Copyright 2021 DeepMind Technologies Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Trainer for CIFAR-10 using PyTorch XLA on TPUs."""
import os

from absl import app
from absl import flags
import torch
from torch import nn
from torch import optim
import torch_xla  # pylint: disable=unused-import
from torch_xla.core import xla_model
from torch_xla.debug import metrics
from torch_xla.distributed import parallel_loader
from torch_xla.distributed import xla_multiprocessing as xmp
import torchvision
from torchvision import datasets
from torchvision import transforms

FLAGS = flags.FLAGS
flags.DEFINE_integer('epochs', 5, 'epochs')
flags.DEFINE_integer('batch_size', 128, 'batch size')
flags.DEFINE_float('learning_rate', 0.1, 'learning rate')
flags.DEFINE_float('momentum', 0.9, 'momentum')

flags.DEFINE_string('platform', 'cpu', 'cpu/gpu/tpu')

_DATA_DIR = '/tmp/cifar10/'
_SERIAL_EXEC = xmp.MpSerialExecutor()


def get_dataset():
  """Download the datasets."""
  transform = transforms.Compose([
      transforms.RandomCrop(32, padding=4),
      transforms.RandomHorizontalFlip(),
      transforms.ToTensor(),
      transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
  ])
  train_dataset = datasets.CIFAR10(
      root=_DATA_DIR, train=True, download=True, transform=transform
  )
  test_dataset = datasets.CIFAR10(
      root=_DATA_DIR, train=False, download=True, transform=transform
  )

  return train_dataset, test_dataset


def train(model, loader, optimizer, loss_fn):
  """Train the model."""
  model.train()
  for images, labels in loader:
    optimizer.zero_grad()
    output = model(images)
    loss = loss_fn(output, labels)
    loss.backward()
    xla_model.optimizer_step(optimizer)


def test(model, loader):
  """Test the model for accuracy."""
  model.eval()
  total = 0
  correct = 0
  with torch.no_grad():
    for images, labels in loader:
      output = model(images)
      predictions = output.max(1, keepdim=True)[1]
      total += labels.size(0)
      correct += predictions.eq(labels.view_as(predictions)).sum().item()

  return correct / total


def _mp_fn(_, args):
  """Multiprocessing main function to call."""
  torch.manual_seed(0)

  # Using the serial executor avoids multiple processes
  # to download the same data.
  train_dataset, test_dataset = _SERIAL_EXEC.run(get_dataset)

  train_sampler = torch.utils.data.distributed.DistributedSampler(
      train_dataset,
      num_replicas=xla_model.xrt_world_size(),
      rank=xla_model.get_ordinal(),
      shuffle=True,
  )
  train_loader = torch.utils.data.DataLoader(
      train_dataset,
      batch_size=args['batch_size'],
      sampler=train_sampler,
      drop_last=True,
  )
  test_loader = torch.utils.data.DataLoader(
      test_dataset, batch_size=args['batch_size'], shuffle=False, drop_last=True
  )

  device = xla_model.xla_device()
  resnet_model = torchvision.models.resnet18(pretrained=False)
  wrapped_model = xmp.MpModelWrapper(resnet_model)
  model = wrapped_model.to(device)

  optimizer = optim.SGD(
      model.parameters(), lr=args['learning_rate'], momentum=args['momentum']
  )
  loss_fn = nn.NLLLoss()

  for epoch in range(args['epochs']):
    para_loader = parallel_loader.ParallelLoader(train_loader, [device])
    train(model, para_loader.per_device_loader(device), optimizer, loss_fn)
    para_loader = parallel_loader.ParallelLoader(test_loader, [device])
    accuracy = test(model, para_loader.per_device_loader(device))
    xla_model.master_print('Finished training epoch {}'.format(epoch))
    xla_model.master_print(
        '[xla:{}] Accuracy={:.2f}%'.format(xla_model.get_ordinal(), accuracy),
        flush=True,
    )
    xla_model.master_print(metrics.metrics_report(), flush=True)


def main(_):
  nprocs = 1
  if FLAGS.platform == 'gpu':
    os.environ['GPU_NUM_DEVICES'] = str(torch.cuda.device_count())
    os.environ['XLA_FLAGS'] = '--xla_gpu_cuda_data_dir=/usr/local/cuda/'
    nprocs = 1
  elif FLAGS.platform == 'tpu':
    # Only import tensorflow to get TPUClusterResolver()
    import tensorflow as tf  # pylint: disable=g-import-not-at-top

    cluster = tf.distribute.cluster_resolver.TPUClusterResolver()
    print('TPU master:', cluster.master())
    master = cluster.master().split('://')[-1]
    os.environ['XRT_TPU_CONFIG'] = f'tpu_worker;0;{master}'
    nprocs = 8

  # We convert FLAGS to a dict, so it can be passed to spawned processes.
  # When calling spawn, FLAGS will be reset and the parsed values are lost.
  # The FLAGS object cannot be passed via args because it cannot be pickled.
  args = dict(FLAGS.__flags)  # pylint: disable=protected-access
  for k in args:
    args[k] = args[k].value if args[k].present else args[k].default

  xmp.spawn(
      _mp_fn,
      nprocs=nprocs,
      start_method='fork',
      args=(args,),
  )


if __name__ == '__main__':
  app.run(main)
