[![Build Status](https://github.com/etsy/boundary-layer/workflows/Pull%20request%20test/badge.svg)](https://github.com/etsy/boundary-layer/actions?query=workflow%3A%22Pull+request+test%22)[![Coverage Status](https://coveralls.io/repos/github/etsy/boundary-layer/badge.svg?branch=master&kill_cache=1)](https://coveralls.io/github/etsy/boundary-layer?branch=master)
# boundary-layer
`boundary-layer` is a tool for building [Airflow](https://www.github.com/apache/incubator-airflow) DAGs from human-friendly, structured, maintainable yaml configuration.  It includes first-class support for various usability enhancements that are not built into Airflow itself:
 - Managed resources created and destroyed by Airflow within a DAG: for example, ephemeral DAG-scoped hadoop clusters on [Dataproc](https://cloud.google.com/dataproc/)
 - Type checking and automatic preprocessing on all arguments to all operators, based on flexible schemas
 - Automatic imports of required classes
 - Distinct `before` and `after` operator groups, to make it easier to manage actions taken at the beginning or end of workflows
 - DAG pruning, for extracting or eliminating sections of the graph while maintaining dependency relationships

`boundary-layer` also performs various checks to find errors that would only be made visible upon deployment to an Airflow instance, such as cycles in the DAG, duplicate task names, etc.

`boundary-layer` is used heavily on the Etsy Data Platform.  Every DAG on our platform is defined by a `boundary-layer` configuration instead of in raw python, which greatly reduces the barrier to entry for our data scientists and engineers to develop DAGs, while ensuring that best practices are always observed in the generated python code.  `boundary-layer` is the core of our fully self-service deployment process, in which DAGs are tested by our CI tools and errors are surfaced prior to allowing DAGs to be merged and deployed to our Airflow instances.

In addition, our migration from Oozie to Airflow relied heavily on `boundary-layer`'s included conversion tool.

`boundary-layer` is _pluggable_, supporting custom configuration and extensions via plugins that are installed using `pip`.  The core package does not contain any etsy-specific customizations; instead, those are all defined in an internally-distributed etsy plugin package.

For more information, see our article on Etsy's [Code as Craft](https://www.etsy.com/codeascraft/boundary-layer-declarative-airflow-workflows?ref=codeascraft) blog.

## Supported operators and Airflow versions

`boundary-layer` requires that each operator have a configuration file to define its schema, the python class it corresponds to, etc.  These configuration files are stored in the [boundary-layer-default-plugin](boundary_layer_default_plugin/config/operators).  We currently include configurations for a number of common Airflow operators (sufficient to support our needs at Etsy, plus a few more), but we know that we are missing quite a few operators that may be needed to satisfy common Airflow use cases.  We are committed to continuing to add support for more operators, and we also commit to supporting a quick turn-around time for any contributed pull requests that only add support for additional operators.  So please, submit a pull request if something is missing, or at least drop an [issue](https://github.com/etsy/boundary-layer/issues) to let us know.

Furthermore, due to some differences in the operators and sensors between Airflow release versions, there may be incompatibilities between `boundary-layer` and some Airflow versions.  All of our operators are known to work with Airflow release versions 1.9 and 1.10 (although our schemas validate against the operator arguments for 1.10, which is a superset of those for 1.9 --- there could be some parameters that we allow but that 1.9 will not properly use).

## Installation

`boundary-layer` is distributed via [PyPI](https://pypi.org/project/boundary-layer/) and can be installed using pip.

```
pip install boundary-layer --upgrade
```

We recommend installing into a [virtual environment](https://docs.python-guide.org/dev/virtualenvs/), but that's up to you.

You should now be able to run `boundary-layer` and view its help message:
```
$ boundary-layer --help
```
If the installation was successful, you should see output like:
```
usage: boundary-layer [-h] {build-dag,prune-dag,parse-oozie} ...

positional arguments:
  {build-dag,prune-dag,parse-oozie}

optional arguments:
  -h, --help            show this help message and exit
```

## Publishing updates to PyPI (admins only)
`boundary-layer` is distributed via [PyPI](https://pypi.org/project/boundary-layer/).  We rely on an automated Github Actions [build](.github/workflows/python-publish.yml) to publish updates.  The build runs every time a tag is pushed to the repository.  We have a [script](release.py) that automates the creation of these tags, making sure that they are versioned correctly and created for the intended commits.

The recommended process for publishing a relatively minor boundary layer update is to simply run
```
./release.py
```
which will bump the patch version.

For bigger changes, you can bump the minor (or major) versions, or you can force a specific version string, via one of the following commands:
```
./release.py --bump minor
./release.py --bump major
./release.py --force-version a.b.c
```

There are a few other options supported by the `release.py` command, as described by the usage string:
```
╰$ ./release.py --help
usage: release.py [-h]
                  [--bump {major,minor,patch} | --force-version FORCE_VERSION]
                  [--git-remote-name GIT_REMOTE_NAME]
                  [--remote-branch-name REMOTE_BRANCH_NAME]

optional arguments:
  -h, --help            show this help message and exit
  --bump {major,minor,patch}
                        Select the portion of the version string to bump.
                        default: `patch`
  --force-version FORCE_VERSION
                        Force the new version to this value. Must be a valid
                        semver.
  --git-remote-name GIT_REMOTE_NAME
                        Name of the git remote from which to release. default:
                        `origin`
  --remote-branch-name REMOTE_BRANCH_NAME
                        Name of the remote branch to use as the basis for the
                        release. default: `master`
```

# boundary-layer YAML configs
The primary feature of boundary-layer is its ability to build python DAGs from simple, structured YAML files.

Below is a simple boundary-layer yaml config, used for running a Hadoop job on Google Cloud Dataproc:
```yaml
name: my_dag
dag_args:
  schedule_interval: '@daily'
resources:
- name: dataproc-cluster
  type: dataproc_cluster
  properties:
    cluster_name: my-cluster-{{ execution_date.strftime('%s') }}
    num_workers: 10
    region: us-central1
default_task_args:
  owner: etsy-data-platform
  project_id: my-project-id
  retries: 2
  start_date: '2018-10-31'
  dataproc_hadoop_jars:
  - gs://my-bucket/my/path/to/my.jar
before:
- name: data-sensor
  type: gcs_object_sensor
  properties:
    bucket: my-bucket
    object: my/object
operators:
- name: my-job
  type: dataproc_hadoop
  requires_resources:
  - dataproc-cluster
  properties:
    main_class: com.etsy.my.job.ClassName
    dataproc_hadoop_properties:
      mapreduce.map.output.compress: 'true'
    arguments: [ '--date', '{{ ds }}' ]
```
A few interesting features:
 - The `resources` section of the configuration defines a transient `DataProc` cluster resource that is required by the hadoop job.  `boundary-layer` will automatically insert the operators to create and delete this cluster, as well as the dependencies between the jobs and the cluster, when the DAG is created.
 - The `before` section of the configuration defines sensors that will be inserted by `boundary-layer` as prerequisites for all downstream operations in the DAG, including the creation of the transient DataProc cluster.

To convert the above YAML config into a python DAG, save it to a file (for convenience, this DAG is already stored in the [examples directory](examples/readme_example.yaml)) and run
```
$ boundary-layer build-dag readme_example.yaml > readme_example.py
```
and, if all goes well, this will write a valid Airflow DAG into the file  `readme_example.py`.  You should open this file up and look at its contents, to get a feel for what boundary-layer is doing.  In particular, after some comments at the top of the file, you should see something like this:
```python
import os
from airflow import DAG

import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.operators.dataproc_operator import DataprocClusterDeleteOperator, DataProcHadoopOperator, DataprocClusterCreateOperator

DEFAULT_TASK_ARGS = {
        'owner': 'etsy-data-platform',
        'retries': 2,
        'project_id': 'my-project-id',
        'start_date': '2018-10-31',
        'dataproc_hadoop_jars': ['gs://my-bucket/my/path/to/my.jar'],
    }

dag = DAG(
        schedule_interval = '@daily',
        catchup = True,
        max_active_runs = 1,
        dag_id = 'my_dag',
        default_args = DEFAULT_TASK_ARGS,
    )

data_sensor = GoogleCloudStorageObjectSensor(
        dag = (dag),
        task_id = 'data_sensor',
        object = 'my/object',
        bucket = 'my-bucket',
        start_date = (datetime.datetime(2018, 10, 31, 0, 0)),
    )


dataproc_cluster_create = DataprocClusterCreateOperator(
        dag = (dag),
        task_id = 'dataproc_cluster_create',
        num_workers = 10,
        region = 'us-central1',
        cluster_name = "my-cluster-{{ execution_date.strftime('%s') }}",
        start_date = (datetime.datetime(2018, 10, 31, 0, 0)),
    )

dataproc_cluster_create.set_upstream(data_sensor)

my_job = DataProcHadoopOperator(
        dag = (dag),
        task_id = 'my_job',
        dataproc_hadoop_properties = { 'mapreduce.map.output.compress': 'true' },
        region = 'us-central1',
        start_date = (datetime.datetime(2018, 10, 31, 0, 0)),
        cluster_name = "my-cluster-{{ execution_date.strftime('%s') }}",
        arguments = ['--date','{{ ds }}'],
        main_class = 'com.etsy.my.job.ClassName',
    )

my_job.set_upstream(dataproc_cluster_create)

dataproc_cluster_destroy_sentinel = DummyOperator(
        dag = (dag),
        start_date = (datetime.datetime(2018, 10, 31, 0, 0)),
        task_id = 'dataproc_cluster_destroy_sentinel',
    )

dataproc_cluster_destroy_sentinel.set_upstream(my_job)

dataproc_cluster_destroy = DataprocClusterDeleteOperator(
        dag = (dag),
        task_id = 'dataproc_cluster_destroy',
        trigger_rule = 'all_done',
        region = 'us-central1',
        cluster_name = "my-cluster-{{ execution_date.strftime('%s') }}",
        priority_weight = 50,
        start_date = (datetime.datetime(2018, 10, 31, 0, 0)),
    )

dataproc_cluster_destroy.set_upstream(my_job)
```

This python DAG is now ready for ingestion directly into a running Airflow instance, following whatever procedure is appropriate for your Airflow deployments.

A few things to note:
 - `boundary-layer` converted the `start_date` parameter from a string to a python `datetime` object.  This is an example of the boundary-layer argument-preprocessor feature, which allows config parameters to be specified as user-friendly strings and converted to the necessary python data structures automatically.
 - `boundary-layer` added a `sentinel` node in parallel with the cluster-destroy node, which serves as an indicator to Airflow itself regarding the ultimate outcome of the Dag Run.  Airflow determines the Dag Run status from the leaf nodes of the DAG, and normally the cluster-destroy node will always execute (irrespective of upstream failures) and will likely succeed. This would cause DAGs with failures in critical nodes to be marked as successes, if not for the sentinel node.  The sentinel node will only trigger if all of its upstream dependencies succeed --- otherwise it will be marked as `upstream-failed`, which induces a failure state for the Dag Run.

# Oozie Migration tools

In addition to allowing us to define Airflow workflows using YAML configurations, `boundary-layer` also provides a module for converting Oozie XML configuration files into `boundary-layer` YAML configurations, which can then be used to create Airflow DAGs.

Admittedly, `boundary-layer`'s Oozie support is currently limited: it is only capable of building DAGs that submit their Hadoop jobs to [Dataproc](https://cloud.google.com/dataproc/) (it does not support stand-alone Hadoop clusters, for example), and it does not support Oozie coordinators.  We are open to working on improved Oozie support if there is community demand for it, and of course, we are open to community contributions toward this goal.

The following command will translate an [example Oozie workflow](test/data/oozie-workflows/example) to a boundary-layer DAG that will execute on a 64-node Dataproc cluster in GCP's `us-east1` region, for the GCP project `my-project-id`:
```
boundary-layer parse-oozie example \
  --local-workflow-base-path test/data/oozie-workflows/ \
  --cluster-project-id my-project-id \
  --cluster-region us-east1 \
  --cluster-num-workers 64
```
