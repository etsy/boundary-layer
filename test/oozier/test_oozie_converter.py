from argparse import Namespace
from boundary_layer.oozier.parse import OozieWorkflowParser
from boundary_layer.oozier.file_fetcher import LocalFileFetcher


def test_workflow_parser():
    workflow_dir = "test/data/oozie-workflows"
    file_fetcher = LocalFileFetcher(workflow_dir)

    oozie_parser = OozieWorkflowParser(
        file_fetcher=file_fetcher,
        prune_forks=False,
        prune_joins=False,
        debug=False)

    args = Namespace(
        dag_concurrency=16,
        dag_max_active_runs=1,
        dag_schedule_interval='@daily',
        dag_disable_catchup=False,
        dag_name=None,
        prune_nodes=None,
        only_nodes=None,
        with_external_task_sensors=[],
        )

    workflow = oozie_parser.load_workflow(
        "example",
        args)

    assert workflow is not None
