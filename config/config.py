# use in production env
# code_dir = "/home/airflow/gcs/dags/"

import os
import yaml


class Config:
    """Configuration class of constant variables."""

    def __init__(self):
        """Sets Class attributes based on key-value paris in config.yaml"""
        config_path = os.path.join(os.getcwd(), "config/config.yaml")
        # use in production env
        # config_path = os.path.join(code_dir, "config/config.yaml")
        with open(config_path, "r") as file:
            base = yaml.safe_load(file)

        for k, v in base.items():
            setattr(self, k, v)
