from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
  'start_date': datetime(2023, 1, 1),
}

with DAG(dag_id='energy-pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         tags=['spark'],
         catchup=False) as dag:

    #preprocess
    preprocess = SparkSubmitOperator(
        application="/Users/kimsy/energy_data_lstm/preprocess.py", task_id="preprocess", conn_id="spark_local"
    )

    #save_model
    save_model = SparkSubmitOperator(
        application="/Users/kimsy/energy_data_lstm/save_model.py", task_id="save_model", conn_id="spark_local"
    )

    #job3
        #save_model
    predict = SparkSubmitOperator(
        application="/Users/kimsy/energy_data_lstm/predict.py", task_id="predict", conn_id="spark_local"
    )

    preprocess >> save_model >> predict
