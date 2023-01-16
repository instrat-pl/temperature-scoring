import pandas as pd

from temperature_scoring.config import data_dir


if __name__ == "__main__":

    dfs = pd.read_excel(
        data_dir("raw", "Dane - spółki z ogłoszonymi celami .xlsx"), sheet_name=None
    )

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
    # Remove reported base year S3 emissions
    df["base_year_ghg_s3"] = pd.NaT
    dfs["target_data"] = df[columns]

    companies = dfs["target_data"]["company_id"].drop_duplicates()
    for key in ["fundamental_data", "portfolio_data"]:
        df = dfs[key]
        dfs[key] = df[df["company_id"].isin(companies)]

    with pd.ExcelWriter(data_dir("clean", "input_data.xlsx")) as writer:
        for key, df in dfs.items():
            df.to_excel(writer, sheet_name=key, index=False)
