
import argparse, os
import pandas as pd




def aggregate_chunk(df, agg_state):
    df["month"] = pd.to_datetime(df["transaction_datetime"], errors="coerce").dt.to_period("M").astype(str)
    g = df.groupby(["month","customer_no"], observed=True)["amount"].sum()
    if agg_state is None:
        return g
    return (pd.concat([agg_state, g])
              .groupby(level=[0,1], observed=True).sum())

def finalize_top10(agg_series):
    agg = agg_series.reset_index(name="total_spend")
    out = (agg.sort_values(["month","total_spend","customer_no"], ascending=[True,False,True])
             .groupby("month", group_keys=False).head(10))
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--chunksize", type=int, default=100_000)
    args = ap.parse_args()

    agg_state = None
    usecols = ["transaction_datetime","customer_no","amount"]
    dtypes = {"customer_no":"string","amount":"float64","transaction_datetime":"string"}

    for chunk in pd.read_csv(args.input, usecols=usecols, dtype=dtypes, chunksize=args.chunksize):
        agg_state = aggregate_chunk(chunk, agg_state)

    result = finalize_top10(agg_state)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    result.to_csv(args.output, index=False)

if __name__ == "__main__":
    main()
