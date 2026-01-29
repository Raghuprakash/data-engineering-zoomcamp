import sys

print('arguments',sys.argv)

month = int(sys.argv[1])

print(f"hello pipeline, month = {month}")

# Add pandas
import pandas as pd

df = pd.DataFrame({"Day": [1, 2], "No.of passengers": [3, 4]})
print(df.head())

df.to_parquet(f"output_month_{sys.argv[1]}.parquet")

#df.to_parquet(f"output_day_{sys.argv[1]}.parquet")

