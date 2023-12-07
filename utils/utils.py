import logging
import requests
from config.config import Config
#only for production env
# import sys

# code_dir = "/home/airflow/gcs/dags/"
# sys.path.append(code_dir)

# from config.config import Config
# from google.cloud import bigquery
# from google.oauth2 import service_account
import logging


class Utils:
    def __init__(self):
        """Loads all attributes from the Config (as cfg) class"""
        self.config = Config()

    def make_request(self, method, url, payloads):
        if method == "GET":
            try:
                response = requests.get(url, params=payloads)
                return response
            except Exception as e:
                logging.error(f"Error while making {method} request to {url}: {e}")
            return None

    def recusrively_search_item(self, key, data):
        """serarch the key and return its corresponding value
        in a json file
        """
        results = []
        if isinstance(data, dict):
            if key in data:
                results.append(data[key])
            for k in data:
                results.extend(self.recusrively_search_item(key, data[k]))
        elif isinstance(data, list):
            for item in data:
                results.extend(self.recusrively_search_item(key, item))
        return results
    
    def df_to_table(
            self,
            df,
            destination_dataset,
            destination_table,
            write_method,
            **kwargs
    ):
        """Writes content of a dataframe to destination table
           in BigQuery

        Args:
            df: Input dataframe
            destination_dataset: Destination dataset name
            destination_table: Destination table name
            write_method: Type of write method ("TRUNCATE OR APPEND")
            **kwargs: Optionally takes schema argument as a list

        Returns:
            N/A

        """
        destination_table_full = self.dataset_table(
            project_id=self.cfg.PROJECT_ID,
            dataset=destination_dataset,
            table_name=destination_table
        )

        if 'schema' in kwargs:
            job_config = bigquery.LoadJobConfig(
                schema=kwargs['schema']
            )
        else:
            job_config = bigquery.LoadJobConfig()

        if write_method == "TRUNCATE":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif write_method == "APPEND":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        else:
            raise ValueError()

        query_job = self.client.load_table_from_dataframe(
            df,
            destination_table_full,
            job_config=job_config
        )
        query_job.result()
        logging.info(
            "Dataframe results loaded to the table {}".format(
                destination_table_full)
        )
    
    def dataset_table(
        self,
        project_id,
        dataset,
        table_name
    ):
        """Return full name of a table in bigquery

        Args:
            project_id: Project id
            dataset: Name of dataset
            table_name: Name of table

        Returns:
            Full name of BigQuery table

        """
        return project_id + "." + dataset + "." + table_name

