"""Performance and Scalability Tips
===================================

This example highlights a few simple patterns to keep df-eval pipelines
efficient and scalable.

It demonstrates:

- Reusing an :class:`df_eval.Engine` instance
- Using :meth:`Engine.evaluate_many` instead of many single calls
"""

import time

import pandas as pd

from df_eval import Engine


# %%
# Build a Moderately Sized DataFrame
# ----------------------------------

n = 50_000
df = pd.DataFrame({"a": range(n), "b": range(n, 2 * n)})
df.head()


# %%
# Reuse a Single Engine Instance
# ------------------------------

engine = Engine()


def time_many_single_calls() -> float:
    start = time.perf_counter()
    for _ in range(20):
        engine.evaluate(df, "a + b")
    return time.perf_counter() - start


def time_evaluate_many() -> float:
    start = time.perf_counter()
    engine.evaluate_many(
        df,
        {
            "sum": "a + b",
            "product": "a * b",
            "avg": "(a + b) / 2",
        },
    )
    return time.perf_counter() - start


single_time = time_many_single_calls()
batch_time = time_evaluate_many()

print("Time for many single evaluate calls: {:.4f}s".format(single_time))
print("Time for a single evaluate_many call: {:.4f}s".format(batch_time))

