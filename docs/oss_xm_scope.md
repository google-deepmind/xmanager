# Scope of OSS XManager

## What's included

Internally within Alphabet, XManager is a fully-featured ecosystem aimed to
facilitate the experimentation process that is deeply integrated with our
internal infrastructure. The XManager repository that you are seeing only
contains a small subset of our ecosystem. Due to the amount of work needed and
coupling with internal tools, we have decided to only open source the launch
API: the means to describe what an experiment consists of and how to launch it.
As a result, users can launch these experiments locally, on Google Cloud
Platform or on Kubernetes. The API can be extended to support other Cloud
platforms.

## The scope

The project’s primary goal is to facilitate collaboration between internal
researchers and the scientific community. The Open Sourced XManager API provides
a way to share our internally-developed code, enabling everybody to reproduce
results, build new knowledge on top of them, or put the code to work for the
benefit of society. It also enables closer collaborations between Alphabet and
external researchers.

As we are focused on delivering our internal roadmap, we may not have bandwidth
for external feature requests, even if they come with an implementation.
XManager was designed to be highly flexible and adaptable and has a
lot of potential for further use cases. Therefore we don’t exclude the
possibility of increasing its scope going forward.

## Can I use XManager for research unrelated to Google or DeepMind?

Yes. XManager API is distributed under Apache 2.0 licence which gives you a lot
of flexibility. We understand that our
[contribution policy](https://github.com/deepmind/xmanager/blob/main/CONTRIBUTING.md)
doesn't sound very reassuring, but we can suggest the following to counter the
risks:

*   The licence allows you to have and maintain a fork with all the changes you
    need.
*   The API has been designed to be modular. Similar to how `xmanager.xm_local`
    extends `xmanager.xm` and provides support for orchestrating experiments
    from a local machine, you may have your own module tailoring the API to your
    needs. In fact this is how our internal version adds support for
    Alphabet-specific infrastructure.
*   The core `xmanager.xm` composes the basic building blocks of a research
    experiment that we think every implementation should be based on. And one of
    the things we've learned over years is that research is a fast moving field.
    It was in our best interest to design it generic, flexible, and extensible.

## What is not included

The following was intentionally left outside of the XManager scope to allow it
to be shared in a reasonable time frame. Note that while these features are
important parts of the research infrastructure we believe that many of them are
better to be provided as a collection of well-integrated libraries / services
rather than an all-in-one tool.

*   Web user interface. Vertex AI and TensorBoard provide good alternatives if
    you need a UI.
*   Metrics storage. XManager API doesn't cover storing, retrieving, or
    conducting analysis of experiment metrics.
*   Computational resource sharing. `xm_local` supports running many experiments
    in the Cloud in parallel. But it doesn't enforce any policy for sharing
    resources between many researchers.
*   Continuous status tracking. The base XManager implementation does not track
    the running status of trials or jobs. It does not contain any mechanism for
    push-notification or notifying a user of success or failure.
