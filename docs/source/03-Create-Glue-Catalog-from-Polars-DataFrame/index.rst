Create Glue Catalog from Polars DataFrame
==============================================================================


Overview
------------------------------------------------------------------------------
在创建AWS Glue Catalog 的过程中, 常常遇到的一个问题就是其复杂的语法. Glue Catalog 的元数据和 key-value结构非常繁琐, 一旦定义错误, 可能会导致创建失败. 或者表虽然创建成功了, 但数据无法被正确查询. AWS Glue 的文档内容庞杂且每个细节都至关重要, 如果一个 key 或者 value 不正确, 可能就会出问题.

`AWS Wrangler <https://aws-sdk-pandas.readthedocs.io/>`_ 是 AWS 官方维护的一个基于 pandas 的数据处理库. 它有一个功能是通过 Pandas DataFrame 里的 Schema 来创建对应的 Glue Catalog. 这里有一个问题是 Pandas 的类型系统由于历史原因导致设计不够完善. 例如如果有一个 column 的值里有 None, 那么即使这个 column 是 integer type 但在 pandas 中还是会被视为 float type. 另外对于复杂嵌套数据结构 list / struct 的支持也不够好. 经常会出现你用这个库创建的 Catalog 无法被 Athena 查询的情况.

本篇博客将探讨如何通过使用 Polars DataFrame (一个高性能的 DataFrame 库) 来替代传统的 Pandas DataFrame, 结合 AWS Wrangler 库, 来简化 Glue Catalog 的创建过程.


Challenge
------------------------------------------------------------------------------
在创建 Glue Catalog 时, 你需要定义数据表的 schema 和表的属性. 其中, schema 是定义数据结构 (如列名和数据类型), 而表的属性则包括数据格式、数据存储位置等信息. 这些信息往往以复杂的 key-value 对的形式呈现, 需要非常精准的配置. 稍有差错, 你的表可能无法被正确使用.

此外, AWS Crawler 服务往往会自动推断 schema 类型, 但这种推断通常并不准确. 对于某些数据类型的推断结果可能会导致问题, 例如字符串可能被识别为数值或时间戳. 相比之下, 手动定义 schema 可以提供更高的准确性和控制.


Solution
------------------------------------------------------------------------------
为了简化 Glue Catalog 的创建过程, AWS Wrangler 库提供了很好的封装, 它能够自动处理很多复杂的元数据和表的属性定义工作. 通过将 Polars DataFrame 与 AWS Wrangler 结合使用, 可以有效地减少手动定义的复杂性.

以下是使用 Polars DataFrame 和 AWS Wrangler 的主要优势:

1. **通过 Polars 自动生成数据表的 schema**: Polars 以其高性能著称, 能够比 Pandas 更高效地处理大规模数据集. 你可以利用 Polars 的 schema 系统来自动生成 Glue Catalog 所需的 schema. 通过将你的样本数据加载到 Polars DataFrame 中, 可以准确地提取出符合 Glue 需求的 schema, 而不必依赖 AWS 的自动推断.
2. **手动控制 schema 定义**: 虽然 AWS Crawler 的服务可以自动推断数据类型, 但这种推断往往不够精确. 通过 Polars, 你可以显式地定义 schema, 确保数据类型准确无误. 这避免了自动推断可能带来的数据类型误差, 特别是在处理复杂数据集时, 这种控制显得尤为重要.
3. **使用 AWS Wrangler 简化表属性的配置**: 除了 schema 以外, Glue Catalog 的表还需要配置很多额外的属性, 如数据格式 (例如 Parquet, CSV 等), 压缩类型, 分区键等. 这些属性通常以复杂的 key-value 对形式出现, 配置起来比较麻烦. 而 AWS Wrangler 封装了这些复杂性, 通过更高级的函数接口简化了配置过程, 减少了手动操作的风险.


Example
------------------------------------------------------------------------------
.. dropdown:: example1.py

    .. literalinclude:: ./example1.py
       :language: python
       :linenos:

.. dropdown:: example2.py

    .. literalinclude:: ./example1.py
       :language: python
       :linenos:
