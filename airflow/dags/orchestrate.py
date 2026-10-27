from airflow.sdk import dag, task

DBT = "cd /opt/airflow/walmart_project && dbt --no-partial-parse {cmd} --profiles-dir /opt/airflow/walmart_project"
PIPELINE_ID = "b0411e4b-a746-4480-96b2-2b3df97ec7a4"


@dag()
def orchestrate():

    @task
    def ingest_cdc():
        import os
        import time
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.errors import ResourceConflict

        host = os.environ["DATABRICKS_HOST"]
        if not host.startswith("http"):
            host = "https://" + host

        ws = WorkspaceClient(host=host, token=os.environ["DATABRICKS_TOKEN"])

        # Start the pipeline directly (no job needed). full_refresh stays off for CDC.
        # If an update is already running, wait on that one instead of failing.
        try:
            update_id = ws.pipelines.start_update(
                pipeline_id=PIPELINE_ID, full_refresh=False
            ).update_id
            print(f"Started pipeline update: {update_id}")
        except ResourceConflict:
            update_id = ws.pipelines.get(pipeline_id=PIPELINE_ID).latest_updates[0].update_id
            print(f"An update is already active, waiting on it: {update_id}")

        deadline = time.time() + 3600  # give up after 1 hour

        while time.time() < deadline:
            state = ws.pipelines.get_update(
                pipeline_id=PIPELINE_ID, update_id=update_id
            ).update.state.value
            print(f"Pipeline state: {state}")

            if state == "COMPLETED":
                return update_id
            if state in ("FAILED", "CANCELED"):
                raise Exception(f"Pipeline update ended with state: {state}")
            time.sleep(10)

        raise TimeoutError("Pipeline update did not finish within 1 hour")

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target /opt/airflow/walmart_project/logs"

    @task.bash
    def source_freshness():
        return DBT.format(cmd="source freshness")

    @task.bash
    def silver_technical():
        return DBT.format(cmd="run --select silver_t")

    @task.bash
    def silver_technical_tests():
        return DBT.format(cmd="test --select silver_t")

    @task.bash
    def silver_business():
        return DBT.format(cmd="run --select silver_b")

    @task.bash
    def silver_business_tests():
        return DBT.format(cmd="test --select silver_b")

    @task.bash
    def gold_ephemeral():
        return DBT.format(cmd="run --select gold/ephemeral")

    @task.bash
    def gold_dimensions():
        return DBT.format(cmd="snapshot")

    @task.bash
    def gold_facts():
        return DBT.format(cmd="run --select gold/facts")

    (
        ingest_cdc()
        >> clean_target()
        >> source_freshness()
        >> silver_technical()
        >> silver_technical_tests()
        >> silver_business()
        >> silver_business_tests()
        >> gold_ephemeral()
        >> gold_dimensions()
        >> gold_facts()
    )


orchestrate()