"""
Out-of-Memory Parquet Processing
================================

This example demonstrates parquet-in/parquet-out workflows where a Parquet
file is treated as an out-of-memory DataFrame.

Pipeline:

- Read source Parquet in row chunks
- Apply schema-driven derived columns per chunk
- Write transformed chunks to a destination Parquet file
- Optionally collect transformed rows in-memory for inspection
"""

from pathlib import Path
from tempfile import mkdtemp
import shutil

import pandas as pd

from df_eval import Engine, iter_parquet_row_chunks

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover - docs environment dependent
    raise ImportError(
        "This example requires pyarrow. Install with: pip install 'df-eval[parquet]'"
    ) from exc


# %%
# Build Source Data
# -----------------
#
# Create a small in-memory DataFrame and write it to a temporary Parquet file.

temp_path = Path(mkdtemp(prefix="df_eval_parquet_example_"))
input_path = temp_path / "input.parquet"
output_path = temp_path / "output.parquet"

source_df = pd.DataFrame(
    {
        "price": [10.0, 12.5, 9.0, 8.5, 15.0],
        "qty": [2, 1, 4, 3, 2],
    }
)
source_table = pa.Table.from_pandas(source_df, preserve_index=False)
pq.write_table(source_table, input_path)

# %%
# Configure Engine and Schema
# ---------------------------

engine = Engine()
schema = {
    "line_total": "price * qty",
    "line_total_with_fee": "line_total + 1.5",
}

# %%
# Run Parquet-In / Parquet-Out
# ----------------------------
#
# Stream row chunks from input Parquet, apply schema-derived columns,
# and write transformed chunks to output Parquet.

result_path = engine.apply_schema_parquet_to_parquet(
    input_path,
    output_path,
    schema,
    chunk_size=2,
    input_columns=["price", "qty"],
    compression="snappy",
)
print(f"Wrote transformed parquet to: {result_path}")

# %%
# Inspect Output Chunks
# ---------------------
#
# Read the transformed Parquet back in chunks and show a combined preview.

output_chunks = list(iter_parquet_row_chunks(output_path, chunk_size=3))
output_preview = pd.concat(output_chunks, ignore_index=True)
print(output_preview)

# %%
# In-Memory Convenience Path
# --------------------------
#
# For smaller datasets, collect transformed chunks into a single DataFrame.

in_memory_result = engine.apply_schema_parquet_to_df(
    input_path,
    schema,
    chunk_size=2,
    input_columns=["price", "qty"],
)
print(in_memory_result)

# %%
# Cleanup Temporary Files
# -----------------------
#
# Remove temporary files created by this example.

shutil.rmtree(temp_path, ignore_errors=True)


