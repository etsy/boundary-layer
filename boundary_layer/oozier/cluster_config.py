import abc


class HadoopClusterConfig(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def managed_resource(self):
        pass

    @abc.abstractproperty
    def apply_config_properties(self, operator_properties, config_properties):
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

    def apply_config_properties(self, operator_properties, config_properties):
        raise NotImplementedError(
            'Sorry, the class `{}` is not fully implemented right now'.format(
                self.__class__.__name__))

    def __init__(self, namenode):
        self.namenode = namenode


class DataprocHadoopClusterConfig(HadoopClusterConfig):
    mapreduce_operator_type = 'dataproc_hadoop'
    hive_operator_type = 'dataproc_hive'

    resource_type = 'dataproc_cluster'
    resource_name = 'dataproc-cluster'

    @property
    def managed_resource(self):
        return {
            'name': self.resource_name,
            'type': self.resource_type,
            'properties': self.create_operator_args,
        }

    def apply_config_properties(self, operator_properties, config_properties):
        result = operator_properties.copy()

        if config_properties:
            result['dataproc_hadoop_properties'] = config_properties

        return result

    def __init__(self, **create_operator_args):
        self.create_operator_args = create_operator_args
