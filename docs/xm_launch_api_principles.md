# XManager launch API principles

XManager is a platform for packaging, running and keeping track of machine
learning experiments. Our primary use case is a human (researcher) launching
experimental setup, making alterations based on outcomes and repeating the
process. Automated feedback-loop pipelines are outside of our scope. This
document describes the design considerations behind the XManager launch API. In
other words, it covers what makes our API specially tailored for the research
needs.

## Tailored for research

ML experiments may vary from single-process binaries running on local GPU to
impressive setups spanning across hundreds of hosts in a cloud. They may consist
of homogeneous processes (e.g. for distributed training on many accelerators) or
contain jobs performing specific roles. For example reinforcement learning
setups would typically contain a learner (trains neural network), agents
(executes the policy encoded in the network and generates input for the learner)
and environments (inside which agents act). When doing research one would
usually run these computations many times with different parameters while
evolving the code over time. It is crucial to be able to easily spin up copies
of the experiment setup.

While there are many tools for describing and deploying such setups, they mostly
come from software engineering land and were created to describe production
services (e.g. web app with frontend, backend, some background queue workers).
XManager was designed to cater research needs.

## Python as a config language

Python is the language of choice for ML researchers. Instead of making them
learn yet another config language we allow describing experiments in the
language researchers already know. This also gives us a great extensibility:
there is a natural way to interact with systems other than XManager from launch
scripts. Using a general purpose language makes experiment definitions less
inspectable and potentially very complex. But it also gives ultimate
flexibility, allows researchers to build on top of our APIs and tailor
abstraction levels to their needs.

## Modular design: prefer building blocks over a monolith

Research is about trying new ideas, we must be open to changes and prototyping
should be cheap. We approach this by avoiding building a unified description of
an experiment. Instead we provide the means to describe individual aspects and
ensure that while they interlock well, any part can be replaced / wrapped with a
domain-specific implementation or even used independently. This allows
frameworks to focus on specific areas without the boilerplate needed to forward
the rest of functionality. This also facilitates prototyping by making the API
more extensible and “hackable”.

<details>
  <summary>Concrete examples of this approach:</summary>

*   The experiment is created by a separate call. Frameworks can accept a ready
    to use `xm.Experiment` and focus on what should be launched inside. It also
    means that creating a new experiment or adding more work units to an
    existing one is syntactically the same, simplifying integration with
    optimizers, such as Vizier.
*   The execution structure (processes launched and their command line
    parameters) is not required to match the experiment structure (work units
    and their hyperparameters). With
    [JobGenerators](https://github.com/deepmind/xmanager/blob/6d3809ed36c070cc07c84e31ca24f37b5fbc19af/xmanager/xm/job_blocks.py#L320)
    one can define an arbitrary mapping from work unit parameters to underlying
    process structure and how to encode parameters.
*   `xmanager.xm` provides a generic API structure, but support for concrete
    backends (such as GCP) is provided by “implementation” modules, such as
    `xm_local`. Multiple implementations of the XM API may coexist and launch
    scripts usually can be migrated between them with minor changes.
*   We recognize a need to store and manage experiment metadata. But we have
    realized that metadata management is orthogonal to the launch process and
    objects created by XManager API (experiments, work units) are not the only
    parts of the research process that can have metadata.
*   We believe that metadata management can be provided by a separate library. For now OSS XManager API provides a rudimental `xm.MetadataContext` class.
    </details>

## Extensibility

Research shouldn’t be locked to a specific Cloud provider. We tried to distill
common, transferable notions to `xmanager.xm`. Support for specific backends
(such as GCP) is provided by “implementation” modules, such as `xm_local` and
cloud-specific settings are represented by dedicated
[Executor](https://github.com/deepmind/xmanager/blob/main/docs/executors.md)
objects. In particular this is how
[Borg](https://research.google.com/pubs/pub43438.html?hl=es) support is provided
in the internal XManager version.

## Eager execution

Explicit machine-inspectable representations, such as Protocol Buffers, GCL or
YAML configs are appealing from the system implementation point of view. But
they are not user-friendly, especially when it comes to describing complex logic
or control flows. The evolution from a graph in TensorFlow 1 to eager execution
in TensorFlow 2 and JAX is a vivid illustration from an adjacent field.

We optimize our API for user convenience and embrace the eager approach: launch
scripts interactively build and update the experiment. The Python script is the
machine-interpretable representation of the experiment. We believe that the
gained ease of use and flexibility are well worth the increased complexity of
our infrastructure. Especially since our user base may have more experience with
applied science than with software engineering. An example of this eager
approach is that creating experiments, packaging the binaries and launching work
units are all done by separate calls.

## Avoid late bindings

Explicit is better than implicit. Experiments need to adapt to external
conditions, for example binaries may need different build flags depending on
which hardware they run. It is tempting to implement this in a form of magic
which would inject the right values when they become available, e.g. having
tokens (like environmental variables) in command line arguments which would be
replaced with actual values later. It is not always possible to avoid such late
bindings, but we apply the following tests:

*   Python already provides rich and familiar means for parametrization,
    including f-strings. Do we need to make users learn that substitutions in
    this specific place are done in a custom way? Is there a need for yet
    another language?
*   Would late bindings work in all needed contexts? What if the value is inside
    a pickled object packaged in a container? What if changes are non trivial
    e.g. binaries need to be compiled differently?

It is often possible to break the data dependency loop and provide a concrete
value. Being explicit slightly bloats the code. But we found that defaults that
magically work for the majority of users tend to obscurely break on edge cases.
This presents a challenge since cutting-edge research often implies working with
edge cases.

## [REPL](https://en.wikipedia.org/wiki/Read%E2%80%93eval%E2%80%93print_loop) speed & repeatability

As interactive research is our primary use case,
[read–eval–print](https://en.wikipedia.org/wiki/Read%E2%80%93eval%E2%80%93print_loop)
loop speed and ability to retrospectively tell what changes were made between
two experiments are important. These are non-functional requirements for
workflows and tooling in general, part of which XManager API is. Researchers
need to deploy cloud setups that compete in size with large production services,
yet they iterate on them how software engineers would iterate on a local
machine. This leads to impedance mismatch with tools designed to rollout
large-scale changes safely and gradually. It is important for the infrastructure
to allow fast iteration.

But while doing so we must maintain enough order to not get lost in the process.
It should be possible to look back and tell which changes were helpful and which
broke our model. So the infrastructure needs to guide users towards good
engineering practices.
