# -*- coding: utf-8 -*-

"""
这个脚本用于验证 polars 转 pandas 时, 数据的 Schema 会不会错乱.

**结论**

大部分情况没有问题, 但当 int 列有 None 时, 会导致 int 列被解读为 float. 这是因为 pandas
的底层是 numpy, 而 numpy 中的 None (NAN) 是一个特殊的浮点数, 所以会导致整个列都被解读为 float.
"""

import polars as pl
import pandas as pd

df = pl.DataFrame(
    [
        {
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
            "a_int": 1,
            "a_str": None,
            "a_int_list": [1, None, 3],
            "a_str_list": None,
            "a_list_of_int_list": [[1, None, 3], None],
            "a_list_of_str_list": [["a", None, "c"], None],
            "a_list_of_struct": [
                {
                    "a_int": 1,
                    "a_str": None,
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
df.write_ndjson("polars.json")
pdf: pd.DataFrame = df.to_pandas(use_pyarrow_extension_array=True)
pdf.to_json("pandas.json", orient="records", lines=True)
