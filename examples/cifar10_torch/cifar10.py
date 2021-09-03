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
"""Trainer for CIFAR-10 using PyTorch on GPUs.

This script has been tested with
image: nvidia/cuda:10.1
pip install torch==1.6.0+cu101 torchvision==0.7.0+cu101

CUDA >= 10.2 returns an NCCL error.
"""

from absl import app
from absl import flags
import numpy as np
import torch
from torch import distributed
from torch import multiprocessing
from torch import nn
from torch import optim
from torch.nn import parallel
from torch.utils import data
import torchvision
from torchvision import transforms

# pylint: disable=g-import-not-at-top
try:
  from xmanager.cloud import utils as caip_utils
except ModuleNotFoundError:
  import caip_utils  # a copy of caip_utils.py is present in the directory.
# pylint: enable=g-import-not-at-top

FLAGS = flags.FLAGS
flags.DEFINE_string('master_addr_port', None, 'master address and port.')
flags.DEFINE_integer('world_size', 1,
                     'Number of nodes/clusters participating in the job.')
flags.DEFINE_integer('rank', 0, 'Rank of the current node/cluster.')

flags.DEFINE_string('train_dir', '/tmp/cifar10/train', 'train directory')
flags.DEFINE_string('test_dir', '/tmp/cifar10/test', 'test directory')

flags.DEFINE_integer('epochs', 5, 'epochs')
flags.DEFINE_integer('batch_size', 128, 'batch size')
flags.DEFINE_float('learning_rate', 0.1, 'learning rate')
flags.DEFINE_float('momentum', 0.9, 'momentum')


def train(model, train_loader, device, optimizer, criterion):
  """Train the model."""
  model.train()
  for images, labels in train_loader:
    images, labels = images.to(device), labels.to(device)
    optimizer.zero_grad()
    outputs = model(images)
    loss = criterion(outputs, labels)
    loss.backward()
    optimizer.step()


def test(model, device, test_loader):
  """Test the model."""
  model.eval()
  total = 0
  correct = 0
  with torch.no_grad():
    for images, labels in test_loader:
      images, labels = images.to(device), labels.to(device)
      output = model(images)
      predictions = output.max(1, keepdim=True)[1]
      total += labels.size(0)
      correct += predictions.eq(labels.view_as(predictions)).sum().item()
  return correct / total


def main_worker(gpu, master_addr, master_port, world_size, node_rank,
                ngpus_per_node, args):
  """The main method each spawned process runs."""
  world_size = world_size * ngpus_per_node
  world_rank = node_rank * ngpus_per_node + gpu
  tcp_address = f'tcp://{master_addr}:{master_port}'
  distributed.init_process_group(
      backend='nccl',
      init_method=tcp_address,
      world_size=world_size,
      rank=world_rank)

  # It is the user's responsibility that each process has the same model.
  torch.manual_seed(0)
  np.random.seed(0)

  batch_size = args['batch_size'] // ngpus_per_node
  model = torchvision.models.resnet18(pretrained=False)
  device = torch.device('cuda:{}'.format(gpu))
  model = model.to(device)
  model = parallel.DistributedDataParallel(model, device_ids=[gpu])

  transform = transforms.Compose([
      transforms.RandomCrop(32, padding=4),
      transforms.RandomHorizontalFlip(),
      transforms.ToTensor(),
      transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
  ])
  train_set = torchvision.datasets.CIFAR10(
      root=args['train_dir'], train=True, download=False, transform=transform)
  test_set = torchvision.datasets.CIFAR10(
      root=args['test_dir'], train=False, download=False, transform=transform)
  train_sampler = data.distributed.DistributedSampler(dataset=train_set)
  train_loader = data.DataLoader(
      dataset=train_set,
      pin_memory=True,
      batch_size=batch_size,
      sampler=train_sampler)
  test_loader = data.DataLoader(
      dataset=test_set, pin_memory=True, shuffle=False)

  criterion = nn.CrossEntropyLoss()
  optimizer = optim.SGD(
      model.parameters(), lr=args['learning_rate'], momentum=args['momentum'])

  for epoch in range(args['epochs']):
    print('[rank:{}] epoch {}'.format(world_rank, epoch))
    train(model, train_loader, device, optimizer, criterion)
    accuracy = test(model, device, test_loader)
    print('[rank:{}] accuracy: {}'.format(world_rank, accuracy))


def main(_):
  # Download the dataset from the main process only once.
  # Otherwise, each spawned process will try to download to the same directory.
  torchvision.datasets.CIFAR10(root=FLAGS.train_dir, train=True, download=True)
  torchvision.datasets.CIFAR10(root=FLAGS.test_dir, train=False, download=True)

  master_addr = None
  master_port = None
  if FLAGS.master_addr_port is not None:
    [master_addr, master_port] = FLAGS.master_addr_port.split(':')
  world_size = FLAGS.world_size
  node_rank = FLAGS.rank
  if master_addr is None or master_port is None:
    master_addr, master_port = caip_utils.get_master_address_port()
    world_size, node_rank = caip_utils.get_world_size_rank()
  ngpus_per_node = torch.cuda.device_count()

  # We convert FLAGS to a dict, so it can be passed to spawned processes.
  # When calling spawn, FLAGS will be reset and the parsed values are lost.
  # The FLAGS object cannot be passed via args because it cannot be pickled.
  args = dict(FLAGS.__flags)  # pylint: disable=protected-access
  for k in args:
    args[k] = args[k].value if args[k].present else args[k].default

  multiprocessing.spawn(
      main_worker,
      nprocs=ngpus_per_node,
      args=(master_addr, master_port, world_size, node_rank, ngpus_per_node,
            args))


if __name__ == '__main__':
  app.run(main)
