from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.bash import BashOperator


class IngestionPipeline:
    def __init__(self, dataset_id, dataset_path):
        self._dataset_id = dataset_id
        self._dataset_path = dataset_path

    def build(self):
        # placeholder for task which will be doing data enrichment
        enrich_data = BashOperator(
            task_id=f'{self._dataset_id}_enrich_data',
            bash_command='echo "This is a placeholder for task which will be doing data enrichment"'
        )

        # placeholder for task which will be doing data ingestion to the warehouse
        ingest_to_snowflake = BashOperator(
            task_id=f'{self._dataset_id}_ingest_data',
            bash_command='echo "This is a placeholder for task which will be doing data ingestion to the warehouse"'
        )

        # placeholder for task which will report metrics to Kafka
        report_kafka_metrics = BashOperator(
            task_id=f'{self._dataset_id}_report_metrics',
            bash_command='echo "This is a placeholder for task which will report metrics to Kafka"'
        )

        enrich_data >> ingest_to_snowflake >> report_kafka_metrics
