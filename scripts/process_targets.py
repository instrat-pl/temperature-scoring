import pandas as pd

from temperature_scoring.config import data_dir


def process_targets(suffix=""):

    input_file = data_dir("clean", f"input_data{suffix}.xlsx")
    df = pd.read_excel(input_file, sheet_name="target_data")

    # Allow Grupa Kęty's intensity target
    df = df[(df["target_type"] == "Absolute") | (df["company_name"] == "Grupa Kęty")]

    # Since Enea has small S2 emissions, assume S1 target is S1+S2 target
    df.loc[(df["company_name"] == "Enea") & (df["scope"] == "S1"), "scope"] = "S1+S2"

    # Aggregate Grupa Azoty S1+S2 target
    df_azoty = df[df["company_name"] == "Grupa Azoty"].copy()

    for scope in ["S1", "S2"]:
        is_scope = df_azoty["scope"] == scope
        df_azoty.loc[is_scope, "reduction_ambition"] *= df_azoty.loc[
            is_scope, f"base_year_ghg_{scope.lower()}"
        ] / df_azoty.loc[is_scope, ["base_year_ghg_s1", "base_year_ghg_s2"]].sum(axis=1)
    df_azoty = (
        df_azoty.groupby(["company_name", "base_year", "end_year"])
        .agg({"reduction_ambition": "sum"})
        .reset_index()
    )
    df_azoty["scope"] = "S1+S2"
    df = pd.concat([df, df_azoty])

    df = df[df["scope"].isin(["S1+S2", "S1+S2+S3"])]

    df = df[["company_name", "reduction_ambition", "base_year", "end_year"]]

    # Assume there is one base year for all company's targets
    assert len(set(df["company_name"])) == len(
        set(df["company_name"] + df["base_year"].astype(str))
    )

    df_base = df[["company_name", "base_year"]].drop_duplicates()
    df_base["emissions"] = 1
    df_base = df_base.rename(columns={"base_year": "year"})

    df["emissions"] = (1 - df["reduction_ambition"]).round(2)
    df = df.rename(columns={"end_year": "year"})
    df = df.drop(columns=["base_year", "reduction_ambition"])

    df = pd.concat([df_base, df])

    df = df.pivot(
        index="year", columns="company_name", values="emissions"
    ).reset_index()

    df.to_csv(data_dir("clean", "reduction_targets.csv"), index=False)


if __name__ == "__main__":

    process_targets()
