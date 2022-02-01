#
"""
This is an CTC example dag for using the KubernetesPodOperator.
"""
# import logging
# log = logging.getLogger(__name__)

# DAG object - Necesario para instanciar el DAG
# -------------------------------------------------------------------------
from airflow import DAG

# Operators - Necesarios para operar
# -------------------------------------------------------------------------
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

# -------------------------------------------------------------------------
# argumentos para el DAG constructor
# -------------------------------------------------------------------------
dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2021, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# -------------------------------------------------------------------------
dag = DAG(
    'ctc_demo_01', 
    default_args=dag_args,
    description = "Un ejemplo simple",
    schedule_interval=timedelta(minutes=10), 
    tags=['example']
)
# -------------------------------------------------------------------------


# DEFINO LAS TAREAS
# -------------------------------------------------------------------------
start_task = DummyOperator(
	task_id='run_this_first', 
	dag=dag
)

k8s_python_task = KubernetesPodOperator(
	namespace='sandbox-airflow',
        image="python:3.6",
        cmds=["python", "-c"],
        arguments=["print('Salida de la tarea ejecutada en Python: hello world')"],
        labels={"foo": "bar"},
        name="k8s-running-python",
        task_id="calling-k8s-python",
        get_logs=True,
        dag=dag
)

k8s_bash_task = KubernetesPodOperator(
	namespace='sandbox-airflow',
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=["echo 'Salida:' && date && uname -a"],
        labels={"foo": "bar"},
        name="k8s-running-bash",
        # is_delete_operator_pod=False,
        task_id="calling-k8s-bash",
        get_logs=True,
        dag=dag
)

std_bash_task = BashOperator(
    task_id='calling-std-bash',
    bash_command='echo test',
    dag=dag
)

final_task = DummyOperator(
	task_id='run_this_last', 
	dag=dag
)

# DEFINO LA RELACION ENTRE LAS TAREAS
# -------------------------------------------------------------------------
start_task >> [k8s_python_task, k8s_bash_task] >> std_bash_task >> final_task
# La linea anterior es equivalente a:
#    python_task.set_upstream(start)
#    bash_task.set_upstream(start)
