from boundary_layer.oozier.parse import OozieWorkflowParser
from boundary_layer.oozier.file_fetcher import LocalFileFetcher


def test_workflow_parser(parse_oozie_default_args):
    """ Basic test for oozie parser, using example oozie workflow """
    workflow_dir = "test/data/oozie-workflows"
    file_fetcher = LocalFileFetcher(workflow_dir)

    oozie_parser = OozieWorkflowParser(
        file_fetcher=file_fetcher,
        prune_forks=False,
        prune_joins=False,
        debug=False)

    workflow = oozie_parser.load_workflow(
        "example",
        parse_oozie_default_args)

    assert len(workflow.primary['sub_dags']) == 1
    assert workflow.primary['sub_dags'][0]['name'] == 'sub-wf-wordcount'
    assert workflow.primary['sub_dags'][0]['target'] == 'sub-workflow'
    assert workflow.primary['sub_dags'][0]['upstream_dependencies'] == ['fork-node']

    assert len(workflow.secondary) == 1
    assert workflow.secondary[0]['name'] == 'sub-workflow'


def test_workflow_parser_with_prune_forks(parse_oozie_default_args):
    """ Test the same example oozie workflow, but set prune_forks to true
        to make sure that the fork node is removed
    """
    workflow_dir = "test/data/oozie-workflows"
    file_fetcher = LocalFileFetcher(workflow_dir)

    oozie_parser = OozieWorkflowParser(
        file_fetcher=file_fetcher,
        prune_forks=True,
        prune_joins=False,
        debug=False)

    workflow = oozie_parser.load_workflow(
        "example",
        parse_oozie_default_args)

    assert len(workflow.primary['sub_dags']) == 1
    assert workflow.primary['sub_dags'][0]['name'] == 'sub-wf-wordcount'
    assert workflow.primary['sub_dags'][0]['target'] == 'sub-workflow'
    assert not workflow.primary['sub_dags'][0].get('upstream_dependencies')

    assert len(workflow.secondary) == 1
    assert workflow.secondary[0]['name'] == 'sub-workflow'
