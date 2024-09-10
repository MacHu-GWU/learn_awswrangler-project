# -*- coding: utf-8 -*-

"""
这个例子演示了从数据文件直接创建 Glue Catalog 表 (不用 crawler) 的最佳做法.

在这个例子中所有的 field 都是 snake_case, 以符合 AWS Glue Catalog 的命名规范.
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
            "a_int": 1,
            "a_str": "alice",
            "a_int_list": [1, 2, 3],
            "a_str_list": ["a", "b", "c"],
            "a_list_of_int_list": [[1, 2, 3], [4, 5, 6]],
            "a_list_of_str_list": [["a", "b", "c"], ["d", "e", "f"]],
            "a_list_of_struct": [
                {
                    "a_int": 1,
                    "a_str": "hello",
                    "a_int_list": [1, 2, 3],
                    "a_str_list": ["a", "b", "c"],
                },
                {
                    "a_int": 2,
                    "a_str": "world",
                    "a_int_list": [1, 2, 3],
                    "a_str_list": ["a", "b", "c"],
                },
            ],
            "a_struct": {
                "a_int": 1,
                "a_str": "hello",
                "a_int_list": [1, 2, 3],
                "a_str_list": ["a", "b", "c"],
                "a_struct": {
                    "a_int": 1,
                    "a_str": "hello",
                    "a_int_list": [1, 2, 3],
                    "a_str_list": ["a", "b", "c"],
                },
            },
        },
        {
            "id": 2,
            "year": "2002",
            "a_int": None,
            "a_str": "bob",
            "a_int_list": [1, None, 3],
            "a_str_list": None,
            "a_list_of_int_list": [[1, None, 3], None],
            "a_list_of_str_list": [["a", None, "c"], None],
            "a_list_of_struct": [
                {
                    "a_int": None,
                    "a_str": "hello",
                    "a_int_list": [1, None, 3],
                    "a_str_list": ["a", None, "c"],
                },
                {
                    "a_int": 2,
                    "a_str": None,
                    "a_int_list": [1, None, 3],
                    "a_str_list": ["a", None, "c"],
                },
            ],
            "a_struct": {
                "a_int": 1,
                "a_str": None,
                "a_int_list": [1, None, 3],
                "a_str_list": None,
                "a_struct": {
                    "a_int": 1,
                    "a_str": None,
                    "a_int_list": [1, None, 3],
                    "a_str_list": None,
                },
            },
        },
    ]
)

manual_simple_schema = {
    "id": Integer(),
    "a_int": Integer(),
    "a_str": String(),
    "a_int_list": List(Integer()),
    "a_str_list": List(String()),
    "a_list_of_int_list": List(List(Integer())),
    "a_list_of_str_list": List(List(String())),
    "a_list_of_struct": List(
        Struct(
            {
                "a_int": Integer(),
                "a_str": String(),
                "a_int_list": List(Integer()),
                "a_str_list": List(String()),
            }
        )
    ),
    "a_struct": Struct(
        {
            "a_int": Integer(),
            "a_str": String(),
            "a_int_list": List(Integer()),
            "a_str_list": List(String()),
            "a_struct": Struct(
                {
                    "a_int": Integer(),
                    "a_str": String(),
                    "a_int_list": List(Integer()),
                    "a_str_list": List(String()),
                }
            ),
        }
    ),
}
manual_glue_schema = {k: v.to_glue() for k, v in manual_simple_schema.items()}
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
    Athena 不 work, 因为 pandas 里的 int column 如果有 NAN, 就会被视为 double 类型.
    """
    write_to_parquet()
    delete_table()

    # NOTE: awswrangler doesn't recognize pyarrow schema system as pandas DF schema
    # we have to use ``use_pyarrow_extension_array=False``
    pdf: pd.DataFrame = df.to_pandas(use_pyarrow_extension_array=False)
    columns_types, partitions_types = wr.catalog.extract_athena_types(
        df=pdf,
        index=False,
        partition_cols=["year"],
        file_format="parquet",
    )

    wr.catalog.create_parquet_table(
        database=db_name,
        table=tb_name,
        path=s3dir_table_parquet.uri,
        partitions_types=partitions_types,
        columns_types=columns_types,
        boto3_session=bsm.boto_ses,
    )

    add_partition()


def example_02():
    """
    手动定义 schema, 这样是没问题的, 就是有点麻烦.
    """
    write_to_parquet()
    delete_table()

    wr.catalog.create_parquet_table(
        database=db_name,
        table=tb_name,
        path=s3dir_table_parquet.uri,
        partitions_types=partition_schema,
        columns_types=manual_glue_schema,
        boto3_session=bsm.boto_ses,
    )

    add_partition()


def example_03():
    """
    从 polars schema 中自动生成 schema, 这样是最方便的.
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


def example_04():
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
    # example_01()
    # example_02()
    # example_03()
    # example_04()
