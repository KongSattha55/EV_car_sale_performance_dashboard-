"""
Prepare data/raw_dataset.csv for the Mac team pipeline.

Source: cleaned LendingClub loan data from the Loan Default Predictor project.
Selects a relevant column subset, drops duplicates, and takes a stratified
sample (by `default`) so MongoDB Atlas M0 (free tier) and live demos stay fast.
"""

import pandas as pd

SOURCE_PARQUET = (
    "/Users/kongsattha/Documents/Personal Doc/PersoanlProject/"
    "Credit Risk : Loan Default Predictor/data/processed/loans_cleaned.parquet"
)
OUTPUT_CSV = "data/raw_dataset.csv"
SAMPLE_SIZE = 10_000
RANDOM_STATE = 42

COLUMNS = [
    "loan_amnt", "funded_amnt", "term", "int_rate", "installment",
    "grade", "sub_grade", "emp_length", "home_ownership", "annual_inc",
    "verification_status", "issue_d", "purpose", "addr_state", "dti",
    "fico_range_low", "fico_range_high", "open_acc", "pub_rec",
    "revol_bal", "revol_util", "total_acc", "mort_acc", "default",
]


def main():
    df = pd.read_parquet(SOURCE_PARQUET, columns=COLUMNS)
    print(f"Loaded {len(df):,} rows x {len(df.columns)} cols")

    df = df.drop_duplicates()
    print(f"After dropping duplicates: {len(df):,} rows")

    null_counts = df.isnull().sum()
    null_counts = null_counts[null_counts > 0]
    print("Missing values per column:")
    print(null_counts if not null_counts.empty else "  none")

    frac = SAMPLE_SIZE / len(df)
    sample = pd.concat(
        g.sample(frac=frac, random_state=RANDOM_STATE) for _, g in df.groupby("default")
    )
    sample = sample.sample(frac=1, random_state=RANDOM_STATE).reset_index(drop=True)

    sample.insert(0, "loan_id", range(1, len(sample) + 1))

    sample.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote {len(sample):,} rows x {len(sample.columns)} cols to {OUTPUT_CSV}")
    print("Default rate in sample:")
    print(sample["default"].value_counts(normalize=True))


if __name__ == "__main__":
    main()
