# Workflow management

This repository contains a workflow manager and a set of libraries designed
to simplify the implementation of complex workflows via a set of independent
(micro)services that can be independently scaled.

Each workflow is modeled as a sequence of _steps_ with dependencies. A workflow
_job_ is identified by a unique UUID an associated with an optional json
message. Worker services wait for requests on a message bus queue and reply
back to the workflow manager with a json message when a job _step_ is complete.

Workflows can have subtasks. These _tasks_ are children workflows trigger
for each element of a given json list containing UUID elements.

An configuration file example:

```yaml
workflows:
  - name: book-word-counts
    steps:
      - name: book-split
      - name: section-counts
        task: word-counts
        depends: [ book-split ]
      - name: sum-splits
        depends: [ section-counts ]
      - name: store-title
    tasks:
      - name: word-counts
        itemListKey: sections
        steps:
          - name: segment-word-counts

```

As an example assume that the goal of our example workflow is to count words in a book using a collection of micro-services that we want to scale independently.
Our micro-services have access to a message bus they use to receive requests
and a key-value database which they can use to exchange information.

In our example, the workflow is triggered using a uuid and an empty json
payload. The _book-split_ and _store-title_ steps can immediatly execute
since they have an empty dependency list. We assume that the _book-split_
step outputs a json body with a list of section uuids, which correspond
to segments of the book that can be processed independently. Once this step
executes the _word-counts_ sub task is invoked for each _section_ of the
document. Dependent on the number of workers available for this step, the
work of counting the document tokens can be parallelised. The _sum-splits_ step
is then invoked to aggregate the partial counts of each section.

The workflow-manager coordinates the execution of each _job_ by sending messages on a message-bus (RabbitMQ). It is not involved in scheduling the workers or managing the data. This is intentional since it allow it to integrate well
in deployments where there is already a scheduler (e.g. Kubernetes) and a storage solution (e.g. distributed file-system or KV store such as BigTable).

Workers can be coded in any language supported by the message-bus (RabbitMQ).
The worker requriments are as follows:

* A worker should listen on a direct queue (defaults to _step_ name but can be specified);
* Incoming messages contain a _job_ UUID and a 'node' name in a json payload which may also contain additional application specific information;
* The worker should reply to the workflow-manager with a json payload containing the UUID and 'node' name of the incoming message; it may specify additional fields;
* The worker should then 'ACK' the original message from the message bus;
* If the worker processing generates an exception the worker should NACK the job message.

This protocol provides for at-least-once semantics.
