from airflow.sdk import dag, task

DBT = "cd /opt/airflow/walmart_project && dbt --no-partial-parse {cmd} --profiles-dir /opt/airflow/walmart_project"
JOB_ID = 672550021054472


@dag()
def orchestrate():

    @task
    def ingest_cdc():
        import os
        import time
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

        host = os.environ["DATABRICKS_HOST"]
        if not host.startswith("http"):
            host = "https://" + host

        ws = WorkspaceClient(host=host, token=os.environ["DATABRICKS_TOKEN"])

        run_id = ws.jobs.run_now(job_id=JOB_ID).run_id
        deadline = time.time() + 3600  # give up after 1 hour

        while time.time() < deadline:
            state = ws.jobs.get_run(run_id=run_id).state
            print(f"Job state: {state.life_cycle_state}")

            if state.life_cycle_state in (
                RunLifeCycleState.TERMINATED,
                RunLifeCycleState.SKIPPED,
                RunLifeCycleState.INTERNAL_ERROR,
            ):
                if state.result_state == RunResultState.SUCCESS:
                    return "Ingest CDC completed successfully"
                raise Exception(
                    f"Job ended: {state.life_cycle_state}, result: {state.result_state}, "
                    f"message: {state.state_message}"
                )
            time.sleep(5)

        raise TimeoutError("Databricks job did not finish within 1 hour")

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