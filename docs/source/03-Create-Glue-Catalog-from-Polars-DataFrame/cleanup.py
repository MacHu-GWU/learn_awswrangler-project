# -*- coding: utf-8 -*-

"""
清除这个实验所用到的所有 AWS 资源.
"""

from s3pathlib import S3Path, context
from boto_session_manager import BotoSesManager
from aws_console_url.api import AWSConsole
from aws_glue_catalog.api import Database, Table

aws_profile = "bmt_app_dev_us_east_1"
db_name = "create_glue_catalog_test_database"
tb_name = "create_glue_catalog_test_table"

bsm = BotoSesManager(profile_name=aws_profile)
context.attach_boto_session(bsm.boto_ses)
acu = AWSConsole.from_bsm(bsm=bsm)
bucket = f"{bsm.aws_account_alias}-{bsm.aws_region}-data"
s3dir_root = S3Path(f"s3://{bucket}/projects/learn_awswrangler/")
url = s3dir_root.get_regional_console_url(bsm.aws_region)
print(f"s3dir_root: {url}")
url = acu.glue.get_table(table_or_arn=tb_name, database=db_name)
print(f"glue_table: {url}")
url = acu.glue.get_database(database_or_arn=db_name)
print(f"glue_database: {url}")


def delete_table():
    tb = Table.get(glue_client=bsm.glue_client, database=db_name, name=tb_name)
    if tb is not None:
        bsm.glue_client.delete_table(DatabaseName=db_name, Name=tb_name)


def delete_database():
    db = Database.get(glue_client=bsm.glue_client, name=db_name)
    if db is not None:
        bsm.glue_client.delete_database(Name=db_name)


if __name__ == "__main__":
    s3dir_root.delete()
    delete_table()
    delete_database()
