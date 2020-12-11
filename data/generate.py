#!/usr/bin/env python3

import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date

# row group 1
df1 = pd.DataFrame({'one': [1, 2, 3],
                    'two': [1, 2, 3],
                    'three': ['foo', 'bar', 'baz'],
                    'four': [datetime(2018, 1, 1),
                             datetime(2018, 1, 2),
                             datetime(2018, 1, 3)],
                    'five': [date(2018, 1, 1),
                             date(2018, 1, 2),
                             date(2018, 1, 3)],
                    'six': [True, False, True],
                    'seven': [0.5, None, 1.0]})
table1 = pa.Table.from_pandas(df1)

# row group 2
df2 = pd.DataFrame({'one': [4, 5, 6],
                    'two': [4, 5, 6],
                    'three': ['uno', 'dos', 'tres'],
                    'four': [datetime(2018, 1, 4),
                             datetime(2018, 1, 5),
                             datetime(2018, 1, 6)],
                    'five': [date(2018, 1, 4),
                             date(2018, 1, 5),
                             date(2018, 1, 6)],
                    'six': [False, False, False],
                    'seven': [0.5, None, 1.0]})
table2 = pa.Table.from_pandas(df2)

with pq.ParquetWriter('example1.parquet', table1.schema) as writer:
    writer.write_table(table1)
    writer.write_table(table2)

# example2.parquet file
df3 = pd.DataFrame({'one': [1, 3, 5, 7, 9],
                    'two': [2, 4, 6, 8, 0],
                    'three': ['eins', 'zwei', 'drei', 'vier', 'fünf'],
                    'four': [datetime(2018, 1, 1),
                             datetime(2018, 1, 3),
                             datetime(2018, 1, 5),
                             datetime(2018, 1, 7),
                             datetime(2018, 1, 9)],
                    'five': [date(2018, 1, 1),
                             date(2018, 1, 3),
                             date(2018, 1, 5),
                             date(2018, 1, 7),
                             date(2018, 1, 9)],
                    'six': [True, False, True, False, True],
                    'seven': [0.1, None, None, None, None]})
table3 = pa.Table.from_pandas(df3)

with pq.ParquetWriter('example2.parquet', table3.schema) as writer:
    writer.write_table(table3)

# example3.parquet file
df4 = pd.DataFrame({'one': [1, 3],
                    'two': [2, 4],
                    'three': ['eins', 'zwei'],
                    'four': [datetime(2018, 1, 1),
                             datetime(2018, 1, 3),],
                    'five': [date(2018, 1, 1),
                             date(2018, 1, 3)],
                    'six': [True, False]})
table4 = pa.Table.from_pandas(df4)

with pq.ParquetWriter('example3.parquet', table4.schema) as writer:
    writer.write_table(table4)

