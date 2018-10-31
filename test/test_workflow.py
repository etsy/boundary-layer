import os
import pytest
from boundary_layer.workflow import Workflow
from boundary_layer.exceptions import InvalidWorkflowReference, InvalidNodeReference
from boundary_layer.builders import PrimaryDagBuilder, SubDagBuilder, GeneratorBuilder


@pytest.mark.smoke
def test_parse_workflow_with_resources():
    filename = 'test/data/good-dags/simple_dataproc_dag.yaml'
    wf = Workflow.load(filename)

    assert frozenset(node.name for node in wf.specs.graphs.primary.graph.nodes()) == \
        frozenset(
            [

                'Datacopier',
                'dataproc_cluster_create',
                'dataproc_cluster_destroy',
                'dataproc_cluster_destroy-sentinel',
            ]
        )
    assert not wf.specs.graphs.secondary

    print(wf.build_dag(PrimaryDagBuilder, SubDagBuilder, GeneratorBuilder))


@pytest.mark.smoke
def test_parse_workflow_with_subdag():
    filename = 'test/data/good-dags/subdag_test.yaml'
    wf = Workflow.load(filename)

    assert frozenset(node.name for node in wf.specs.graphs.primary.graph.nodes()) == \
        frozenset([
            'tester',
            'Datacopier',
            'SuccessFileSensor',
            'dataproc_cluster_create',
            'dataproc_cluster_destroy',
            'dataproc_cluster_destroy-sentinel'])
    assert len(wf.specs.graphs.secondary) == 1
    assert frozenset(node.name for node in wf.specs.graphs.secondary[0].graph.nodes()) == \
        frozenset(['SubDagSuccessFileSensor'])

    print(wf.build_dag(PrimaryDagBuilder, SubDagBuilder, GeneratorBuilder))


@pytest.mark.smoke
def test_parse_workflow_with_generator():
    filename = 'test/data/good-dags/generator_test.yaml'
    wf = Workflow.load(filename)

    assert frozenset(node.name for node in wf.specs.graphs.primary.graph.nodes()) == \
        frozenset([
            'file_lister',
            'Datacopier',
            'SuccessFileSensor',
            'dataproc_cluster_create',
            'dataproc_cluster_destroy',
        ])
    assert len(wf.specs.graphs.secondary) == 1
    assert frozenset(node.name for node in wf.specs.graphs.secondary[0].graph.nodes()) == \
        frozenset(['SubDagSuccessFileSensor'])

    print(wf.build_dag(PrimaryDagBuilder, SubDagBuilder, GeneratorBuilder))


@pytest.mark.smoke
def test_parse_workflow_with_multiple_generators():
    filename = 'test/data/good-dags/multi_generators.yaml'
    wf = Workflow.load(filename)

    assert frozenset(node.name for node in wf.specs.graphs.primary.graph.nodes()) == \
        frozenset([
            'file_lister',
            'file_lister_1',
            'file_lister_2',
            'file_lister_3',
            'file_lister-6e0a68-fc',
            'file_lister_3-b3341b-fc',
        ])
    assert len(wf.specs.graphs.secondary) == 1
    assert frozenset(node.name for node in wf.specs.graphs.secondary[0].graph.nodes()) == \
        frozenset(['SubDagSuccessFileSensor'])

    print(wf.build_dag(PrimaryDagBuilder, SubDagBuilder, GeneratorBuilder))


@pytest.mark.smoke
def test_parse_invalid_workflows():
    directory = 'test/data/bad-dags'
    for filename in os.listdir(directory):
        path = os.path.join(directory, filename)

        with pytest.raises(Exception):
            Workflow.load(path)


def test_build_referrer_map():
    filename = 'test/data/good-dags/subdag_test.yaml'
    wf = Workflow.load(filename)

    assert wf._build_referrer_map() == {'subdag-tester': [(None, 'tester')]}


def test_node_path_builder():
    filename = 'test/data/good-dags/subdag_test.yaml'
    wf = Workflow.load(filename)

    assert wf._all_node_paths() == {
        (None, 'Datacopier'): [(None, 'Datacopier')],
        (None, 'SuccessFileSensor'): [(None, 'SuccessFileSensor')],
        (None, 'tester'): [(None, 'tester')],
        ('subdag-tester', 'SubDagSuccessFileSensor'): [
            (None, 'tester'),
            ('subdag-tester', 'SubDagSuccessFileSensor')]
    }


def test_invalid_node_reference_errors():
    filename = 'test/data/good-dags/subdag_test.yaml'
    wf = Workflow.load(filename)

    with pytest.raises(InvalidWorkflowReference):
        wf._partitioned_node_paths(node_refs=['invalid-dag.any_node'])

    with pytest.raises(InvalidNodeReference):
        print(wf._partitioned_node_paths(
            node_refs=['subdag-tester.invalid_node']))


def test_node_path_partitioner():
    filename = 'test/data/good-dags/subdag_test.yaml'
    wf = Workflow.load(filename)

    (referenced, complement) = wf._partitioned_node_paths(
        node_refs=['subdag-tester.SubDagSuccessFileSensor'])

    referenced.sort(key=lambda path: path[0][1])
    assert referenced == [
        [
            (None, 'tester'),
            ('subdag-tester', 'SubDagSuccessFileSensor'),
        ]
    ]

    complement.sort(key=lambda path: path[0][1])
    assert complement == [
        [(None, 'Datacopier')],
        [(None, 'SuccessFileSensor')],
        [(None, 'tester')],
    ]


def test_workflow_prune():
    filename = 'test/data/good-dags/subdag_test.yaml'
    wf = Workflow.load(filename)

    pruned_wf = wf.prune(prune_nodes=['subdag-tester.SubDagSuccessFileSensor'])

    assert 'sub_dags' in wf.primary
    assert wf.secondary

    assert 'sub_dags' not in pruned_wf.primary
    assert not pruned_wf.secondary


def test_workflow_prune_only_nodes():
    filename = 'test/data/good-dags/subdag_test.yaml'
    wf = Workflow.load(filename)

    pruned_wf = wf.prune(only_nodes=['subdag-tester.SubDagSuccessFileSensor'])

    assert pruned_wf.primary['sub_dags'] == wf.primary['sub_dags']
    assert pruned_wf.secondary == wf.secondary


def test_workflow_prune_resulting_in_unreachable():
    filename = 'test/data/good-dags/subdag_test.yaml'
    wf = Workflow.load(filename)

    pruned_wf = wf.prune(prune_nodes=['tester'])

    assert 'sub_dags' in wf.primary
    assert wf.secondary

    assert 'sub_dags' not in pruned_wf.primary
    assert not pruned_wf.secondary


def test_only_subdag_operator_includes_sub_workflow():
    filename = 'test/data/good-dags/subdag_test.yaml'
    wf = Workflow.load(filename)

    pruned_wf = wf.prune(only_nodes=['tester'])

    # assert wf.secondary == pruned_wf.secondary

    assert 'sub_dags' in wf.primary
    assert wf.secondary

    assert 'sub_dags' in pruned_wf.primary
    assert pruned_wf.secondary
