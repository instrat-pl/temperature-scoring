import pandas as pd
import numpy as np

from temperature_scoring.config import data_dir


def clean_company_data(raw_file, clean_file):
    df = pd.read_excel(raw_file)
    df = df.rename(
        columns={
            "Sektor": "sector",
            "Spółka": "company_code",
            "Nazwa wg. target data": "company_name",
        }
    )
    df = df[["company_name", "company_code", "ISIN", "sector"]]
    df = df.sort_values(by=["company_name", "company_code"])
    df = df[~df.isna().all(axis=1)]
    df.to_csv(clean_file, index=False)


def clean_emission_data(raw_file, clean_file, year_columns, skiprows):
    dfs = []
    for year, columns in year_columns.items():
        df = pd.read_excel(raw_file, usecols=columns, skiprows=skiprows)
        df = df.rename(columns=lambda x: x.split(".")[0])
        df = df.rename(columns={"scope_2m": "scope_2", "scope_1n2m": "scope_1n2"})
        df = df[~df.isna().all(axis=1)]
        dfs.append(df)

    df = pd.concat(dfs)
    df = df.drop(columns=["scope_2_loc", "scope_2loc"])
    for col in df.columns:
        if not col.startswith("scope"):
            continue
        df[col] = df[col].replace("x", np.nan)
        df[col] = (
            df[col]
            .astype(str)
            .str.replace("\xa0", " ")
            .str.replace(" ", "")
            .str.replace(",", ".")
            .astype(float)
        ).round(2)
    df["scope_1+2"] = df[["scope_1", "scope_2"]].sum(axis=1).round(2)
    df = df.drop(columns="scope_1n2")
    df = df[df["scope_1+2"] > 0]

    df = df.rename(
        columns={
            "ROK": "year",
            "Spółka": "company_code",
            "scope_1": "S1",
            "scope_2": "S2",
            "scope_1+2": "S1+S2",
            "scope_3": "S3",
        }
    )
    df = df.melt(
        id_vars=["year", "company_code"],
        value_vars=["S1", "S2", "S1+S2", "S3"],
        var_name="scope",
        value_name="emissions",
    )
    df["year"] = df["year"].astype(int)

    df = df.pivot(index=["company_code", "scope"], columns="year", values="emissions")
    df = df[~df.isna().all(axis=1)]
    value_columns = df.columns.to_list()
    df = df.reset_index()

    df_names = pd.read_csv(data_dir("clean", "company_data.csv"))
    df = df.merge(df_names, on="company_code", how="left")
    df = df.sort_values(by=["company_name", "company_code"])

    index_columns = [col for col in df.columns if col not in value_columns]
    df = df[index_columns + value_columns]

    df.to_csv(clean_file, index=False)


if __name__ == "__main__":

    raw_file = data_dir("raw", "company_data.xlsx")
    clean_file = data_dir("clean", "company_data.csv")
    clean_company_data(raw_file, clean_file)

    raw_file = data_dir("raw", "Dane ESG GPW.xlsx")
    clean_file = data_dir("clean", "emission_data.csv")

    year_columns = {
        2021: "C:M",
        2020: "W:AE",
        2019: "AI:AP",
        2018: "AT:BA",
    }
    clean_emission_data(raw_file, clean_file, year_columns, skiprows=3)
