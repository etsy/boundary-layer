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
        cluster_base_name=None,
        cluster_depends_on_past=False,
        cluster_name_suffix=None,
        cluster_num_workers=128,
        cluster_project_id='my-project',
        cluster_region='us-central1',
        cluster_wait_for_downstream=False,
        dag_concurrency=16,
        dag_disable_catchup=False,
        dag_max_active_runs=1,
        dag_name=None,
        dag_schedule_interval='@daily',
        only_nodes=None,
        prune_nodes=None,
        with_external_task_sensors=[],
        workflow_name='test-workflow',
        )

    workflow = oozie_parser.load_workflow(
        "example",
        args)

    assert workflow is not None
