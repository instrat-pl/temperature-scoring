import pandas as pd

from temperature_scoring.config import data_dir


def clean_input_data(raw_file, clean_file, use_estimates=False):
    dfs = pd.read_excel(raw_file, sheet_name=None)

    new_keys = {
        "Target Data": "target_data",
        "Fundamental Data": "fundamental_data",
        "Portfolio Data": "portfolio_data",
    }

    dfs = {new_keys[key]: df for key, df in dfs.items()}

    df = dfs["target_data"]
    columns = df.columns
    ghg_columns = [f"base_year_ghg_s{i}" for i in [1, 2, 3]]

    validity_conditions = (
        df["scope"].notna()
        & df["reduction_ambition"].notna()
        & df["base_year"].notna()
        & df["end_year"].notna()
        # & df[ghg_columns].notna().any(axis=1)
    )
    print(
        "These Excel rows are invalid:",
        [i + 2 for i in validity_conditions[~validity_conditions].index],
    )
    dfs["target_data"] = df[validity_conditions].copy()

    # Strip whitespaces in scope
    dfs["target_data"]["scope"] = dfs["target_data"]["scope"].str.strip()

    # Fill GHG emissions by scope where possible
    df = dfs["target_data"]
    for ghg_col in ghg_columns:
        df_ghg = df[["company_id", "base_year", ghg_col]].dropna().drop_duplicates()
        df = df.drop(columns=ghg_col).merge(
            df_ghg, on=["company_id", "base_year"], how="left"
        )
    df = df.drop_duplicates()
    dfs["target_data"] = df[columns]

    # Use estimates in fundamental data
    if use_estimates:
        df = dfs["fundamental_data"]
        df["ghg_s3"] = (
            df["ghg_s3"].fillna(df["base_year_ghg_s3"]).fillna(df["ghg_s3_estimate"])
        )
        dfs["fundamental_data"] = df

    # Assume equal investment of 1 USD
    df = dfs["portfolio_data"]
    df["investment_value"] = 1
    dfs["portfolio_data"] = df

    companies = dfs["target_data"]["company_id"].drop_duplicates()
    for key in ["fundamental_data", "portfolio_data"]:
        df = dfs[key]
        dfs[key] = df[df["company_id"].isin(companies)]

    with pd.ExcelWriter(clean_file) as writer:
        for key, df in dfs.items():
            df.to_excel(writer, sheet_name=key, index=False)


if __name__ == "__main__":

    raw_file = data_dir("raw", "Dane - spółki z ogłoszonymi celami .xlsx")

    clean_file = data_dir("clean", "input_data.xlsx")
    clean_input_data(raw_file, clean_file)

    clean_file = data_dir("clean", "input_data_with_estimates.xlsx")
    clean_input_data(raw_file, clean_file, use_estimates=True)
