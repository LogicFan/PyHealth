# %%
import dask.dataframe as dd
import pandas as pd
import operator
import functools

# Create a small pandas DataFrame
pdf = pd.DataFrame({
    "col1": ["A", "B", "C"],
    "col2": ["X", "Y", "Z"],
    "num": [1, 2, 3]
})

# Convert to Dask DataFrame (1 partition)
df = dd.from_pandas(pdf, npartitions=1)

# %%
# Cast non-string column
ts = functools.reduce(operator.add, (df[col].astype(str) for col in ["col1", "col2", "num"]))

result = ts.compute()
print(result)
# %%
