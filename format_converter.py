import os

import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc


def convert_to_orc(parquet_file: str) -> str:
    df = pd.read_parquet(parquet_file)
    orc_file = parquet_file.replace('.parquet', '.orc')

    table = pa.Table.from_pandas(df)
    orc.write_table(table, orc_file)

    return orc_file


class FormatConverter:
    def __init__(self, data_dir: str = "data", row_count: int = 1000):
        self.data_dir = data_dir
        self.row_count = row_count

    def convert_all_workloads(self):
        workloads = ["core", "bi", "classic", "geo", "log", "ml"]

        for workload in workloads:
            parquet_file = os.path.join(
                self.data_dir,
                f"{workload}_r{self.row_count}_c20_generated.parquet"
            )
            if os.path.exists(parquet_file):
                orc_file = convert_to_orc(parquet_file)
                print(f"Converted {workload}: {orc_file}")
