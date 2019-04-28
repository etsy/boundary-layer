from collections import namedtuple
import pytest
import yaml

from boundary_layer.exceptions import CyclicWorkflowException, InvalidFlowControlNode
from boundary_layer.graph import _GraphUtil
from boundary_layer.registry import RegistryNode, NodeTypes
from boundary_layer.registry.types.operator import OperatorNode

MOCK_NODE_CONFIG_YAML = """
name: mock_node
parameters_jsonschema:
        string_param:
            type: string
        array_param:
            type: array
            items:
                type: string
        dict_param:
            type: object
            additionalProperties:
                type:
                    string
"""


class MockNode(RegistryNode):
    def imports(self):
        return []

    type = NodeTypes.OPERATOR

    def __init__(self, item):
        mock_config = yaml.safe_load(MOCK_NODE_CONFIG_YAML)
        super(MockNode, self).__init__(mock_config, item)


def test_node_comparison():
    node0 = MockNode({'name': 'test', 'type': 'none'})
    node1 = MockNode({'name': 'test', 'type': 'none'})
    node2 = MockNode({'name': 'test2', 'type': 'none'})
    node3 = MockNode({'name': 'test', 'type': 'some'})

    assert node0 == node1
    assert node1 != node2
    assert node1 != node3


def test_graph_builder():
    node0 = OperatorNode(
        {}, {'name': 'test0', 'type': 'none', 'upstream_dependencies': ['test1']})
    node1 = OperatorNode({}, {'name': 'test1', 'type': 'none'})
    node2 = OperatorNode(
        {}, {'name': 'test2', 'type': 'none', 'downstream_dependencies': ['test0']})

    graph = _GraphUtil.build_subgraph([node0, node1, node2])
    assert frozenset(graph.nodes()) == frozenset([node0, node1, node2])

    assert frozenset(graph.predecessors(node0)) == frozenset([node1, node2])
    assert list(graph.successors(node0)) == []

    assert list(graph.predecessors(node1)) == []
    assert list(graph.successors(node1)) == [node0]

    assert list(graph.predecessors(node2)) == []
    assert list(graph.successors(node2)) == [node0]


def test_invalid_upstream_ref():
    node0 = OperatorNode(
        {}, {'name': 'test0', 'type': 'none', 'upstream_dependencies': ['test3']})
    node1 = OperatorNode({}, {'name': 'test1', 'type': 'none'})
    node2 = OperatorNode(
        {}, {'name': 'test2', 'type': 'none', 'downstream_dependencies': ['test0']})

    with pytest.raises(Exception):
        _GraphUtil.build_subgraph([node0, node1, node2])


def test_invalid_downstream_ref():
    node0 = OperatorNode(
        {}, {'name': 'test0', 'type': 'none', 'upstream_dependencies': ['test1']})
    node1 = OperatorNode({}, {'name': 'test1', 'type': 'none'})
    node2 = OperatorNode(
        {}, {'name': 'test2', 'type': 'none', 'downstream_dependencies': ['test4']})

    with pytest.raises(Exception):
        _GraphUtil.build_subgraph([node0, node1, node2])


def test_cycle_detector():
    node0 = OperatorNode(
        {}, {'name': 'test0', 'type': 'none', 'upstream_dependencies': ['test1']})
    node1 = OperatorNode(
        {}, {'name': 'test1', 'type': 'none', 'upstream_dependencies': ['test2']})
    node2 = OperatorNode(
        {}, {'name': 'test2', 'type': 'none', 'upstream_dependencies': ['test0']})

    with pytest.raises(CyclicWorkflowException):
        _GraphUtil.build_subgraph([node0, node1, node2])


def test_boundary_extractor():
    node0 = OperatorNode({}, {'name': 'test0', 'type': 'none'})
    node1 = OperatorNode(
        {}, {'name': 'test1', 'type': 'none', 'upstream_dependencies': ['test0']})
    node2 = OperatorNode(
        {}, {'name': 'test2', 'type': 'none', 'downstream_dependencies': ['test1']})
    node3 = OperatorNode(
        {}, {'name': 'test3', 'type': 'none', 'upstream_dependencies': ['test1']})

    graph = _GraphUtil.build_subgraph([node0, node1, node2, node3])

    upstream = _GraphUtil.get_upstream_surface(graph)
    assert frozenset(upstream) == frozenset([node0, node2])

    downstream = _GraphUtil.get_downstream_surface(graph)
    assert frozenset(downstream) == frozenset([node3])


def test_orderer():
    node0 = OperatorNode({}, {'name': 'test0', 'type': 'none'})
    node1 = OperatorNode(
        {}, {'name': 'test1', 'type': 'none', 'upstream_dependencies': ['test0']})
    node2 = OperatorNode(
        {}, {'name': 'test2', 'type': 'none', 'downstream_dependencies': ['test1']})
    node3 = OperatorNode(
        {}, {'name': 'test3', 'type': 'none', 'upstream_dependencies': ['test1']})

    graph = _GraphUtil.build_subgraph([node0, node1, node2, node3])

    ordered = list(_GraphUtil.ordered(graph))

    assert ordered == [node0, node2, node1, node3] or \
        ordered == [node2, node0, node1, node3]


def test_dependency_set_computations():
    """ Test methods for computing upstream and downstream dependency sets for
        specific nodes in the graph
    """
    node0 = OperatorNode({}, {'name': 'test0', 'type': 'none'})
    node1 = OperatorNode({}, {'name': 'test1', 'type': 'none'})
    node2 = OperatorNode(
        {}, {'name': 'test2', 'type': 'none', 'upstream_dependencies': ['test0', 'test1']})
    node3 = OperatorNode(
        {}, {'name': 'test3', 'type': 'none', 'upstream_dependencies': ['test0', 'test1']})

    graph = _GraphUtil.build_subgraph([node0, node1, node2, node3])

    assert _GraphUtil.downstream_dependency_set(
        node0, graph) == frozenset([node2, node3])
    assert _GraphUtil.downstream_dependency_set(
        node1, graph) == frozenset([node2, node3])
    assert _GraphUtil.upstream_dependency_set(
        node2, graph) == frozenset([node0, node1])
    assert _GraphUtil.upstream_dependency_set(
        node3, graph) == frozenset([node0, node1])


def test_flow_control_inserter_auto_fc_node(fc_node_builder):
    """ Test method for inserting flow control nodes between upstream and downstream
        nodes with auto-created flow-control node
    """
    node0 = OperatorNode({}, {'name': 'test0', 'type': 'none'})
    node1 = OperatorNode({}, {'name': 'test1', 'type': 'none'})
    node2 = OperatorNode(
        {}, {'name': 'test2', 'type': 'none', 'upstream_dependencies': ['test0', 'test1']})
    node3 = OperatorNode(
        {}, {'name': 'test3', 'type': 'none', 'upstream_dependencies': ['test0', 'test1']})

    graph = _GraphUtil.build_subgraph([node0, node1, node2, node3])

    _GraphUtil.attach_flow_control_between(
        upstream=frozenset([node0, node1]),
        downstream=frozenset([node2, node3]),
        graph=graph,
        fc_node_builder=fc_node_builder,
    )

    assert len(graph.nodes()) == 5
    fc_node = [node for node in graph.nodes() if node.name.endswith('-fc')]
    assert len(fc_node) == 1
    assert fc_node[0].type == NodeTypes.OPERATOR
    assert fc_node[0].name == "test2-test3-d454d5-fc"

    assert _GraphUtil.upstream_dependency_set(
        fc_node[0], graph) == frozenset([node0, node1])
    assert _GraphUtil.downstream_dependency_set(
        fc_node[0], graph) == frozenset([node2, node3])

    assert _GraphUtil.downstream_dependency_set(
        node0, graph) == frozenset([fc_node[0]])
    assert _GraphUtil.downstream_dependency_set(
        node1, graph) == frozenset([fc_node[0]])
    assert _GraphUtil.upstream_dependency_set(
        node2, graph) == frozenset([fc_node[0]])
    assert _GraphUtil.upstream_dependency_set(
        node3, graph) == frozenset([fc_node[0]])


def test_flow_control_inserter():
    """ Test method for inserting flow control nodes between upstream and downstream
        nodes with preexisting flow control node
    """
    node0 = OperatorNode({}, {'name': 'test0', 'type': 'none'})
    node1 = OperatorNode({}, {'name': 'test1', 'type': 'none'})
    node2 = OperatorNode(
        {}, {'name': 'test2', 'type': 'none', 'upstream_dependencies': ['test0', 'test1']})
    node3 = OperatorNode(
        {}, {'name': 'test3', 'type': 'none', 'upstream_dependencies': ['test0', 'test1']})
    fc_node = OperatorNode({}, {'name': 'my-fc-node', 'type': 'none'})

    graph = _GraphUtil.build_subgraph([node0, node1, node2, node3])

    _GraphUtil.attach_flow_control_between(
        upstream=frozenset([node0, node1]),
        downstream=frozenset([node2, node3]),
        graph=graph,
        fc_node=fc_node,
    )

    assert len(graph.nodes()) == 5
    assert fc_node in graph.nodes()

    assert _GraphUtil.upstream_dependency_set(
        fc_node, graph) == frozenset([node0, node1])
    assert _GraphUtil.downstream_dependency_set(
        fc_node, graph) == frozenset([node2, node3])

    assert _GraphUtil.downstream_dependency_set(
        node0, graph) == frozenset([fc_node])
    assert _GraphUtil.downstream_dependency_set(
        node1, graph) == frozenset([fc_node])
    assert _GraphUtil.upstream_dependency_set(
        node2, graph) == frozenset([fc_node])
    assert _GraphUtil.upstream_dependency_set(
        node3, graph) == frozenset([fc_node])


def test_flow_control_inserter_invalid_args(fc_node_builder):
    """ Make sure that attach_flow_control_between() raises an exception if
        there are any missing dependency linkages between the upstream and
        downstream nodes
    """
    node0 = OperatorNode({}, {'name': 'test0', 'type': 'none'})
    node1 = OperatorNode({}, {'name': 'test1', 'type': 'none'})
    node2 = OperatorNode(
        {}, {'name': 'test2', 'type': 'none', 'upstream_dependencies': ['test0']})
    node3 = OperatorNode(
        {}, {'name': 'test3', 'type': 'none', 'upstream_dependencies': ['test0', 'test1']})

    graph = _GraphUtil.build_subgraph([node0, node1, node2, node3])

    with pytest.raises(InvalidFlowControlNode):
        _GraphUtil.attach_flow_control_between(
            upstream=frozenset([node0, node1]),
            downstream=frozenset([node2, node3]),
            graph=graph,
            fc_node_builder=fc_node_builder,
        )


def test_resource_boundaries():
    """ Make sure upstream and downstream resource boundary checkers work. These
        find the sets of all nodes that require a specified resource and have no
        upstream (or downstream) nodes that require that resource.  Test it
        with a non-trivial graph --- one in which there is a node in the middle
        that does not require the resource, so that we make sure that we do not
        return merely the set of nodes for which no immediate predecessor or
        successor requires the resource.
    """
    node0 = OperatorNode({}, {
        'name': 'test0',
        'type': 'none',
        'requires_resources': ['cluster'],
    })

    node1 = OperatorNode({}, {
        'name': 'test1',
        'type': 'none',
        'requires_resources': ['cluster'],
    })

    node2 = OperatorNode({}, {
        'name': 'test2',
        'type': 'none',
        'upstream_dependencies': ['test0'],
    })

    node3 = OperatorNode({}, {
        'name': 'test3',
        'type': 'none',
        'upstream_dependencies': ['test2'],
        'requires_resources': ['cluster'],
    })

    node4 = OperatorNode({}, {
        'name': 'test4',
        'type': 'none',
        'upstream_dependencies': ['test2'],
        'requires_resources': ['cluster'],
    })

    graph = _GraphUtil.build_subgraph([node0, node1, node2, node3, node4])

    assert set(_GraphUtil.upstream_resource_boundary(
        'cluster', graph)) == set([node0, node1])
    assert set(_GraphUtil.downstream_resource_boundary(
        'cluster', graph)) == set([node1, node3, node4])


def test_bundler_inserter(fc_node_builder):
    """ Make sure the insert_bundler_nodes() method works properly for a
        general graph, with multiple upstream dependency sets
    """
    node0 = OperatorNode({}, {
        'name': 'test0',
        'type': 'none',
    })

    node1 = OperatorNode({}, {
        'name': 'test1',
        'type': 'none',
    })

    node2 = OperatorNode({}, {
        'name': 'test2',
        'type': 'none',
        'upstream_dependencies': ['test0'],
    })

    node3 = OperatorNode({}, {
        'name': 'test3',
        'type': 'none',
        'upstream_dependencies': ['test1', 'test2'],
    })

    node4 = OperatorNode({}, {
        'name': 'test4',
        'type': 'none',
        'upstream_dependencies': ['test2'],
    })

    graph = _GraphUtil.build_subgraph([node0, node1, node2, node3, node4])

    dep_set_0 = frozenset([node1, node2])
    dep_set_1 = frozenset([node0])

    bundlers = _GraphUtil.insert_bundler_nodes(
        [node2, node3],
        graph,
        fc_node_builder=fc_node_builder)

    assert set(bundlers.keys()) == set([dep_set_0, dep_set_1])

    bundler_0 = bundlers[dep_set_0]
    assert bundler_0.name == 'test1-test2-322ef2-fc'
    assert _GraphUtil.upstream_dependency_set(bundler_0, graph) == dep_set_0
    assert _GraphUtil.downstream_dependency_set(
        bundler_0, graph) == frozenset([node3])

    bundler_1 = bundlers[dep_set_1]
    assert bundler_1.name == 'test0-f6f406-fc'
    assert _GraphUtil.upstream_dependency_set(bundler_1, graph) == dep_set_1
    assert _GraphUtil.downstream_dependency_set(
        bundler_1, graph) == frozenset([node2])


def test_default_bundler_inserter(fc_node_builder):
    """ Make sure the insert_bundler_nodes() method works properly for a
        simpler graph, with a single distinct upstream dependency set and a
        specified default bundler node
    """
    node0 = OperatorNode({}, {
        'name': 'test0',
        'type': 'none',
    })

    node1 = OperatorNode({}, {
        'name': 'test1',
        'type': 'none',
    })

    node2 = OperatorNode({}, {
        'name': 'test2',
        'type': 'none',
        'upstream_dependencies': ['test0'],
    })

    node3 = OperatorNode({}, {
        'name': 'test3',
        'type': 'none',
        'upstream_dependencies': ['test1', 'test2'],
    })

    resource = OperatorNode({}, {
        'name': 'cluster',
        'type': 'none',
    })

    graph = _GraphUtil.build_subgraph([node0, node1, node2, node3])

    dep_set = frozenset([node1, node2])

    bundlers = _GraphUtil.insert_bundler_nodes(
        [node3],
        graph,
        default_bundler_node=resource,
        fc_node_builder=fc_node_builder)

    assert set(bundlers.keys()) == set([dep_set])

    bundler_0 = bundlers[dep_set]
    assert bundler_0 == resource
    assert _GraphUtil.upstream_dependency_set(bundler_0, graph) == dep_set
    assert _GraphUtil.downstream_dependency_set(
        bundler_0, graph) == frozenset([node3])


MockResource = namedtuple(
    'MockResource', [
        'name',
        'create_operator',
        'destroy_operator',
        'disable_sentinel_node'])


def test_attach_create_resource(fc_node_builder):
    """ Make sure upstream resource-create operator insertion works correctly
    """
    node0 = OperatorNode({}, {
        'name': 'test0',
        'type': 'none',
    })

    node1 = OperatorNode({}, {
        'name': 'test1',
        'type': 'none',
    })

    node2 = OperatorNode({}, {
        'name': 'test2',
        'type': 'none',
        'upstream_dependencies': ['test0'],
    })

    node3 = OperatorNode({}, {
        'name': 'test3',
        'type': 'none',
        'upstream_dependencies': ['test2'],
        'requires_resources': ['cluster'],
    })

    node4 = OperatorNode({}, {
        'name': 'test4',
        'type': 'none',
        'upstream_dependencies': ['test2', 'test1'],
        'requires_resources': ['cluster'],
    })

    node5 = OperatorNode({}, {
        'name': 'test4',
        'type': 'none',
        'upstream_dependencies': ['test2'],
        'requires_resources': ['cluster'],
    })

    graph = _GraphUtil.build_subgraph(
        [node0, node1, node2, node3, node4, node5])

    create_resource_node = OperatorNode({}, {
        'name': 'create_cluster',
        'type': 'none',
    })

    resource = MockResource(name='cluster',
                            create_operator=create_resource_node,
                            destroy_operator=None,
                            disable_sentinel_node=None)

    _GraphUtil.attach_create_resource(resource, graph, fc_node_builder)

    assert set(_GraphUtil.upstream_resource_boundary(
        'cluster', graph)) == set([node3, node4, node5])

    cluster_upstream_deps = _GraphUtil.upstream_dependency_set(
        create_resource_node, graph)
    # There should be 2 bundlers because there are 2 distinct dependency sets among
    # the nodes in the upstream resource boundary
    assert len(cluster_upstream_deps) == 2

    node3_upstream = _GraphUtil.upstream_dependency_set(node3, graph)
    assert len(node3_upstream) == 2
    assert _GraphUtil.upstream_dependency_set(node5, graph) == node3_upstream
    assert create_resource_node in node3_upstream
    node3_bundler = list(node3_upstream - frozenset([create_resource_node]))[0]
    assert _GraphUtil.upstream_dependency_set(
        node3_bundler, graph) == frozenset([node2])

    node4_upstream = _GraphUtil.upstream_dependency_set(node4, graph)
    assert len(node4_upstream) == 2
    assert create_resource_node in node4_upstream
    node4_bundler = list(node4_upstream - frozenset([create_resource_node]))[0]
    assert _GraphUtil.upstream_dependency_set(
        node4_bundler, graph) == frozenset([node1, node2])

    create_resource_downstream_deps = _GraphUtil.downstream_dependency_set(
        create_resource_node, graph)
    assert create_resource_downstream_deps == frozenset([node3, node4, node5])


def test_attach_destroy_resource(fc_node_builder):
    """ Make sure downstream resource-delete operator insertion works correctly
    """
    node0 = OperatorNode({}, {
        'name': 'test0',
        'type': 'none',
    })

    node1 = OperatorNode({}, {
        'name': 'test1',
        'type': 'none',
    })

    node2 = OperatorNode({}, {
        'name': 'test2',
        'type': 'none',
        'upstream_dependencies': ['test0'],
    })

    node3 = OperatorNode({}, {
        'name': 'test3',
        'type': 'none',
        'upstream_dependencies': ['test2'],
        'requires_resources': ['cluster'],
    })

    node4 = OperatorNode({}, {
        'name': 'test4',
        'type': 'none',
        'upstream_dependencies': ['test2', 'test1'],
        'requires_resources': ['cluster'],
    })

    node5 = OperatorNode({}, {
        'name': 'test4',
        'type': 'none',
        'upstream_dependencies': ['test3'],
        'requires_resources': ['cluster'],
    })

    graph = _GraphUtil.build_subgraph(
        [node0, node1, node2, node3, node4, node5])

    destroy_resource_node = OperatorNode({}, {
        'name': 'destroy_cluster',
        'type': 'none',
    })

    resource = MockResource(name='cluster',
                            create_operator=None,
                            destroy_operator=destroy_resource_node,
                            disable_sentinel_node=False)

    _GraphUtil.attach_destroy_resource(resource, graph, fc_node_builder)

    destroy_node_upstream = _GraphUtil.upstream_dependency_set(
        destroy_resource_node, graph)

    assert destroy_node_upstream == frozenset([node4, node5])

    node4_downstream = _GraphUtil.downstream_dependency_set(node4, graph)
    assert node4_downstream == _GraphUtil.downstream_dependency_set(
        node4, graph)
    assert len(node4_downstream) == 2  # include the sentinel node

    sentinel_node = list(node4_downstream -
                         frozenset([destroy_resource_node]))[0]
    assert sentinel_node.name == 'destroy_cluster-sentinel'


def test_attach_destroy_resource_no_sentinel_by_config(fc_node_builder):
    """ Make sure downstream resource-delete operator insertion works correctly
    """
    node0 = OperatorNode({}, {
        'name': 'test0',
        'type': 'none',
    })

    node1 = OperatorNode({}, {
        'name': 'test1',
        'type': 'none',
    })

    node2 = OperatorNode({}, {
        'name': 'test2',
        'type': 'none',
        'upstream_dependencies': ['test0'],
    })

    node3 = OperatorNode({}, {
        'name': 'test3',
        'type': 'none',
        'upstream_dependencies': ['test2'],
        'requires_resources': ['cluster'],
    })

    node4 = OperatorNode({}, {
        'name': 'test4',
        'type': 'none',
        'upstream_dependencies': ['test2', 'test1'],
        'requires_resources': ['cluster'],
    })

    node5 = OperatorNode({}, {
        'name': 'test4',
        'type': 'none',
        'upstream_dependencies': ['test3'],
        'requires_resources': ['cluster'],
    })

    graph = _GraphUtil.build_subgraph(
        [node0, node1, node2, node3, node4, node5])

    destroy_resource_node = OperatorNode({}, {
        'name': 'destroy_cluster',
        'type': 'none',
    })

    resource = MockResource(name='cluster',
                            create_operator=None,
                            destroy_operator=destroy_resource_node,
                            disable_sentinel_node=True)

    _GraphUtil.attach_destroy_resource(resource, graph, fc_node_builder)

    destroy_node_upstream = _GraphUtil.upstream_dependency_set(
        destroy_resource_node, graph)

    assert destroy_node_upstream == frozenset([node4, node5])

    assert _GraphUtil.downstream_dependency_set(node4, graph) == \
        _GraphUtil.downstream_dependency_set(node5, graph) == \
        frozenset([destroy_resource_node])


def test_attach_destroy_resource_no_sentinel_because_other_leaf_nodes(fc_node_builder):
    """ Make sure downstream resource-delete operator insertion works correctly
    """
    node0 = OperatorNode({}, {
        'name': 'test0',
        'type': 'none',
    })

    node1 = OperatorNode({}, {
        'name': 'test1',
        'type': 'none',
    })

    node2 = OperatorNode({}, {
        'name': 'test2',
        'type': 'none',
        'upstream_dependencies': ['test0'],
    })

    node3 = OperatorNode({}, {
        'name': 'test3',
        'type': 'none',
        'upstream_dependencies': ['test2'],
        'requires_resources': ['cluster'],
    })

    node4 = OperatorNode({}, {
        'name': 'test4',
        'type': 'none',
        'upstream_dependencies': ['test2', 'test1'],
        'requires_resources': ['cluster'],
    })

    node5 = OperatorNode({}, {
        'name': 'test4',
        'type': 'none',
        'upstream_dependencies': ['test3'],
    })

    graph = _GraphUtil.build_subgraph(
        [node0, node1, node2, node3, node4, node5])

    destroy_resource_node = OperatorNode({}, {
        'name': 'destroy_cluster',
        'type': 'none',
    })

    resource = MockResource(name='cluster',
                            create_operator=None,
                            destroy_operator=destroy_resource_node,
                            disable_sentinel_node=False)

    _GraphUtil.attach_destroy_resource(resource, graph, fc_node_builder)

    destroy_node_upstream = _GraphUtil.upstream_dependency_set(
        destroy_resource_node, graph)

    assert destroy_node_upstream == frozenset([node3, node4])

    assert len(_GraphUtil.downstream_dependency_set(node4, graph)) == 2
    assert destroy_resource_node in _GraphUtil.downstream_dependency_set(
        node4, graph)

    # sentinel node should also have been inserted
    assert any(item.name == 'destroy_cluster-sentinel' for item in
               _GraphUtil.downstream_dependency_set(node4, graph))


def test_prune_node():
    """ Test to make sure that node-pruning works
    """
    node0 = OperatorNode({}, {'name': 'test0', 'type': 'none'})
    node1 = OperatorNode(
        {}, {'name': 'test1', 'type': 'none', 'upstream_dependencies': ['test0']})
    node2 = OperatorNode(
        {}, {'name': 'test2', 'type': 'none', 'upstream_dependencies': ['test0', 'test1']})
    node3 = OperatorNode(
        {}, {'name': 'test3', 'type': 'none', 'upstream_dependencies': ['test0', 'test1']})

    graph = _GraphUtil.build_subgraph([node0, node1, node2, node3])

    _GraphUtil.prune_node(node1, graph)

    assert _GraphUtil.upstream_dependency_set(
        node3, graph) == frozenset([node0])
    assert _GraphUtil.downstream_dependency_set(
        node0, graph) == frozenset([node2, node3])


def test_prune_nodes():
    """ Test to make sure that node-pruning works
    """
    node0 = OperatorNode({}, {'name': 'test0', 'type': 'none'})
    node1 = OperatorNode(
        {}, {'name': 'test1', 'type': 'none', 'upstream_dependencies': ['test0']})
    node2 = OperatorNode(
        {}, {'name': 'test2', 'type': 'none', 'upstream_dependencies': ['test0', 'test1']})
    node3 = OperatorNode(
        {}, {'name': 'test3', 'type': 'none', 'upstream_dependencies': ['test0', 'test1']})

    graph = _GraphUtil.build_subgraph([node0, node1, node2, node3])

    _GraphUtil.prune_nodes(
        graph, node_selector=lambda node: node.name == 'test1')

    assert _GraphUtil.upstream_dependency_set(
        node3, graph) == frozenset([node0])
    assert _GraphUtil.downstream_dependency_set(
        node0, graph) == frozenset([node2, node3])
