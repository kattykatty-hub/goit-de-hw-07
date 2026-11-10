
import random
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule


MYSQL_CONN_ID = "goit_mysql_db_katty"

# Change this to 35 for the required failed-sensor test.
DELAY_SECONDS = 5

# Use a unique results table to avoid conflicts with other students.
RESULTS_TABLE = "olympic_dataset.hw07_katty_medal_results"


def choose_medal():
    return random.choice(["Bronze", "Silver", "Gold"])


def choose_branch(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"


def generate_delay():
    print(f"Waiting {DELAY_SECONDS} seconds")
    time.sleep(DELAY_SECONDS)


with DAG(
    dag_id="hw07_katty_medal_dag",
    description="GoIT HW07 - Apache Airflow medal counting",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "katty",
        "retries": 0,
    },
    tags=["goit", "hw07", "mysql"],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(20) NOT NULL,
                `count` INT NOT NULL,
                created_at DATETIME NOT NULL
            );
        """,
    )

    pick_medal = PythonOperator(
        task_id="pick_medal",
        python_callable=choose_medal,
    )

    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=choose_branch,
    )

    medal_tasks = []

    for medal in ["Bronze", "Silver", "Gold"]:
        task = SQLExecuteQueryOperator(
            task_id=f"calc_{medal}",
            conn_id=MYSQL_CONN_ID,
            sql=f"""
                INSERT INTO {RESULTS_TABLE}
                    (medal_type, `count`, created_at)
                SELECT
                    '{medal}',
                    COUNT(*),
                    NOW()
                FROM olympic_dataset.athlete_event_results
                WHERE medal = '{medal}';
            """,
        )
        medal_tasks.append(task)

    delay_task = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            SELECT
                CASE
                    WHEN TIMESTAMPDIFF(
                        SECOND,
                        MAX(created_at),
                        NOW()
                    ) BETWEEN 0 AND 30
                    THEN 1
                    ELSE 0
                END
            FROM {RESULTS_TABLE};
        """,
        poke_interval=2,
        timeout=8,
        mode="reschedule",
    )

    create_table >> pick_medal >> pick_medal_task

    for task in medal_tasks:
        pick_medal_task >> task
        task >> delay_task

    delay_task >> check_for_correctness
