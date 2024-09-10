# -*- coding: utf-8 -*-

"""
这个脚本用于验证在 polars 转 pandas 时, 如何尽量避免数据拷贝.

# ------------------------------------------------------------------------------
# main1()
# ------------------------------------------------------------------------------
Line #    Mem usage    Increment  Occurrences   Line Contents
=============================================================
19     40.6 MiB     40.6 MiB           1   @profile
20                                         def main1():
21    110.6 MiB     70.0 MiB           1       df = create_df()

# ------------------------------------------------------------------------------
# main2()
# ------------------------------------------------------------------------------
Line #    Mem usage    Increment  Occurrences   Line Contents
=============================================================
31     41.5 MiB     41.5 MiB           1   @profile
32                                         def main2():
36    112.0 MiB     70.5 MiB           1       df = create_df()
37    327.5 MiB    215.5 MiB           1       pdf = df.to_pandas() <--- 215.5 MiB

# ------------------------------------------------------------------------------
# main3()
# ------------------------------------------------------------------------------
Line #    Mem usage    Increment  Occurrences   Line Contents
=============================================================
51     39.8 MiB     39.8 MiB           1   @profile
52                                         def main3():
53    110.0 MiB     70.1 MiB           1       df = create_df()
54    214.2 MiB    104.3 MiB           1       pdf = df.to_pandas(use_pyarrow_extension_array=True) <--- 104.3 MiB

**结论**

用 ``df.to_pandas(use_pyarrow_extension_array=True)`` 是最省内存的.
"""

import os
import polars as pl
from memory_profiler import profile


def create_df():
    n_record = 1_000_000
    df = pl.DataFrame(
        {
            "id": range(1, 1 + n_record),
            "text": [os.urandom(16).hex() for _ in range(n_record)],
        }
    )
    return df


@profile
def main1():
    df = create_df()


@profile
def main2():
    df = create_df()
    pdf = df.to_pandas()


@profile
def main3():
    df = create_df()
    pdf = df.to_pandas(use_pyarrow_extension_array=True)


if __name__ == "__main__":
    """ """
    main1()
    main2()
    main3()
