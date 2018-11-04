import argparse
import abc


class HadoopClusterConfig(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def managed_resource(self):
        pass

    @abc.abstractproperty
    def mapreduce_operator_type(self):
        pass

    @abc.abstractproperty
    def hive_operator_type(self):
        pass


class StandardHadoopClusterConfig(HadoopClusterConfig):
    managed_resource = None
    mapreduce_operator_type = 'mapreduce'
    hive_operator_type = 'hive'

    def __init__(self, namenode):
        self.namenode = namenode


class DataprocHadoopClusterConfig(HadoopClusterConfig):
    mapreduce_operator_type = 'dataproc_hadoop'
    hive_operator_type = 'dataproc_hive'

    @property
    def managed_resource(self):
        properties = {
            'project_id': self.project_id,
            'region': self.region,
            'cluster_name': self.cluster_name,
            'num_workers': self.num_workers,
        }

        return {
            'name': 'dataproc-cluster',
            'type': 'dataproc_cluster',
            'properties': properties,
        }

    def __init__(self, project_id, region, cluster_name, num_workers):
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.num_workers = num_workers
