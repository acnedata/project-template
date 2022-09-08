import os
from dataclasses import dataclass

from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.sdk.api_client import ApiClient

SOURCE_PATH = "dbfs:/FileStore/ica/apple_instagram.csv"
DEST_PATH = "data/apple_instagram.csv"
HOST_ENV_VAR = "DATABRICKS_HOST_ACNE"
TOKEN_ENV_VAR = "DATABRICKS_TOKEN_ACNE"


@dataclass
class DatabricksAPI:
    host: str
    token: str

    def get_file(self, source_path: str, dest_path: str) -> None:
        api_client = ApiClient(host=os.getenv(self.host), token=os.getenv(self.token))
        dbfs_api = DbfsApi(api_client)
        dbfs_path = DbfsPath(source_path)
        dbfs_api.get_file(dbfs_path, dest_path, overwrite=True)


db_api = DatabricksAPI(host=HOST_ENV_VAR, token=TOKEN_ENV_VAR)
db_api.get_file(SOURCE_PATH, DEST_PATH)
