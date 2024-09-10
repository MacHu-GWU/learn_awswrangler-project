# -*- coding: utf-8 -*-

"""
这个例子演示了从数据文件直接创建 Glue Catalog 表 (不用 crawler) 的最佳做法.

在这个例子中所有的 field 都是 camelcase, 并不符合 AWS Glue Catalog 的命名规范.
我们看看在这种情况下应该怎么处理.
"""

import io
import polars as pl
import pandas as pd
import awswrangler as wr
from s3pathlib import S3Path, context
from boto_session_manager import BotoSesManager
from aws_console_url.api import AWSConsole
from aws_glue_catalog.api import Database, Table
from simpletype.api import (
    Integer,
    String,
    List,
    Struct,
    polars_type_to_simple_type,
)

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
s3dir_table_parquet = (s3dir_root / "parquet").to_dir()
s3dir_table_ndjson = (s3dir_root / "ndjson").to_dir()

df = pl.DataFrame(
    [
        {
            "id": 1,
            "year": "2001",
            "aInt": 1,
            "aStr": "alice",
            "aIntList": [1, 2, 3],
            "aStrList": ["a", "b", "c"],
            "aListOfIntList": [[1, 2, 3], [4, 5, 6]],
            "aListOfStrList": [["a", "b", "c"], ["d", "e", "f"]],
            "aListOfStruct": [
                {
                    "aInt": 1,
                    "aStr": "hello",
                    "aIntList": [1, 2, 3],
                    "aStrList": ["a", "b", "c"],
                },
                {
                    "aInt": 2,
                    "aStr": "world",
                    "aIntList": [1, 2, 3],
                    "aStrList": ["a", "b", "c"],
                },
            ],
            "aStruct": {
                "aInt": 1,
                "aStr": "hello",
                "aIntList": [1, 2, 3],
                "aStrList": ["a", "b", "c"],
                "aStruct": {
                    "aInt": 1,
                    "aStr": "hello",
                    "aIntList": [1, 2, 3],
                    "aStrList": ["a", "b", "c"],
                },
            },
        },
        {
            "id": 2,
            "year": "2002",
            "aInt": None,
            "aStr": "bob",
            "aIntList": [1, None, 3],
            "aStrList": None,
            "aListOfIntList": [[1, None, 3], None],
            "aListOfStrList": [["a", None, "c"], None],
            "aListOfStruct": [
                {
                    "aInt": None,
                    "aStr": "hello",
                    "aIntList": [1, None, 3],
                    "aStrList": ["a", None, "c"],
                },
                {
                    "aInt": 2,
                    "aStr": None,
                    "aIntList": [1, None, 3],
                    "aStrList": ["a", None, "c"],
                },
            ],
            "aStruct": {
                "aInt": 1,
                "aStr": None,
                "aIntList": [1, None, 3],
                "aStrList": None,
                "aStruct": {
                    "aInt": 1,
                    "aStr": None,
                    "aIntList": [1, None, 3],
                    "aStrList": None,
                },
            },
        },
    ]
)

partition_schema = {
    "year": String().to_glue(),
}

auto_glue_schema = {
    k: polars_type_to_simple_type(v).to_glue() for k, v in df.schema.items()
}
del auto_glue_schema["year"]


def write_to_parquet():
    s3dir_table_parquet.delete()
    for (year,), sub_df in df.group_by(["year"], maintain_order=True):
        s3path = s3dir_table_parquet.joinpath(f"year={year}/data.parquet")
        buffer = io.BytesIO()
        sub_df.write_parquet(buffer, compression="snappy")
        s3path.write_bytes(buffer.getvalue())


def write_to_json():
    s3dir_table_ndjson.delete()
    for (year,), sub_df in df.group_by(["year"], maintain_order=True):
        s3path = s3dir_table_ndjson.joinpath(f"year={year}/data.json")
        buffer = io.BytesIO()
        sub_df.write_ndjson(buffer)
        s3path.write_bytes(buffer.getvalue())


def create_database():
    db = Database.get(glue_client=bsm.glue_client, name=db_name)
    if db is None:
        db = bsm.glue_client.create_database(DatabaseInput={"Name": db_name})


def delete_table():
    tb = Table.get(glue_client=bsm.glue_client, database=db_name, name=tb_name)
    if tb is not None:
        bsm.glue_client.delete_table(DatabaseName=db_name, Name=tb_name)


def add_partition():
    wr.athena.repair_table(
        table=tb_name,
        database=db_name,
        boto3_session=bsm.boto_ses,
    )


def example_01():
    """
    经过检查, 我们发现 S3 中的数据是 camelcase, 跟原始数据一致.
    而 AWS Glue Catalog 的命名规范是 snakecase, 所以 athena 查询的结果都是 snakecase,
    包裹 struct 的字段名也是 snakecase.
    可见 Glue Catalog 能够自动处理这种情况, 只不过为了保持一致性, 所以结果返回的是 snakecase.
    """
    write_to_parquet()
    delete_table()

    wr.catalog.create_parquet_table(
        database=db_name,
        table=tb_name,
        path=s3dir_table_parquet.uri,
        partitions_types=partition_schema,
        columns_types=auto_glue_schema,
        boto3_session=bsm.boto_ses,
    )

    add_partition()


def example_02():
    """
    再来试试 ndjson 格式, 也是完全没有问题的.
    """
    write_to_json()
    delete_table()

    wr.catalog.create_json_table(
        database=db_name,
        table=tb_name,
        path=s3dir_table_ndjson.uri,
        partitions_types=partition_schema,
        columns_types=auto_glue_schema,
        boto3_session=bsm.boto_ses,
    )

    add_partition()


if __name__ == "__main__":
    """ """
    example_01()
    example_02()
