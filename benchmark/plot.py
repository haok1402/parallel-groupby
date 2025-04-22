import argparse
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

lookup = dict()
lookup["duckdb"] = "https://docs.google.com/spreadsheets/d/1vuWKaN1mRO-rNrv8Nq2--aJkyy7XOSeWHQtkK0rNQyM/export?format=csv&gid=0"
lookup["two-phase-central-merge"] = "https://docs.google.com/spreadsheets/d/1vuWKaN1mRO-rNrv8Nq2--aJkyy7XOSeWHQtkK0rNQyM/export?format=csv&gid=842006617"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--method", type=str, required=True, choices=lookup.keys())
    parsed = parser.parse_args()

    df = pd.read_csv(lookup[parsed.method])
    df = df.dropna()
    print(df)

if __name__ == "__main__":
    main()
