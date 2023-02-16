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
        df_azoty.groupby(
            [
                "company_name",
                "base_year",
                "end_year",
                "base_year_ghg_s1",
                "base_year_ghg_s2",
            ]
        )
        .agg({"reduction_ambition": "sum"})
        .reset_index()
    )
    df_azoty["scope"] = "S1+S2"
    df = pd.concat([df, df_azoty])

    df = df[df["scope"].isin(["S1+S2", "S1+S2+S3"])]

    df["base_year_ghg_s1s2"] = df[["base_year_ghg_s1", "base_year_ghg_s2"]].sum(axis=1)
    df = df[
        [
            "company_name",
            "reduction_ambition",
            "base_year",
            "end_year",
            "base_year_ghg_s1s2",
        ]
    ]

    # Remove net-zero targets for S1+S2+S3 in the long term if there is already a medium/short term net-zero target for S1+S2
    df = df.sort_values(["company_name", "end_year"])
    df = df[~df[["company_name", "reduction_ambition"]].duplicated(keep="first")]

    # Assume there is one base year for all company's targets
    assert len(set(df["company_name"])) == len(
        set(df["company_name"] + df["base_year"].astype(str))
    )

    df_base = df[["company_name", "base_year", "base_year_ghg_s1s2"]].drop_duplicates()

    df_base["relative_emissions"] = 1
    df_base = df_base.rename(columns={"base_year": "year"})

    df["relative_emissions"] = (1 - df["reduction_ambition"]).round(2)
    df = df.rename(columns={"end_year": "year"})
    df = df.drop(columns=["base_year", "reduction_ambition"])

    df = pd.concat([df_base, df])
    df["emissions"] = (df["relative_emissions"] * df["base_year_ghg_s1s2"]).round(2)

    for col in ["emissions", "relative_emissions"]:
        df_res = df.pivot(
            index="year", columns="company_name", values=col
        ).reset_index()
        df_res.to_csv(data_dir("clean", f"{col}_targets.csv"), index=False)


def append_historical_data_to_emission_targets():

    df_targets = pd.read_csv(data_dir("clean", "emissions_targets.csv"))
    companies = df_targets.columns[1:]
    df_targets["type"] = "target"

    df = pd.read_csv(data_dir("clean", "emission_data.csv"))
    df = df[df["scope"] == "S1+S2"]
    df = df[df["company_name"].isin(companies)]

    years = [str(y) for y in range(2018, 2022)]
    df = df.set_index("company_name")[years]
    df.columns.name = "year"
    df = df.transpose().reset_index()
    df["year"] = df["year"].astype(int)
    df["type"] = "historical"

    df = pd.concat([df, df_targets])
    df = df.melt(id_vars=["year", "type"], var_name="company", value_name="emissions")

    df = df.sort_values(["year", "company"])
    df = df[df["emissions"].notna()]
    df["emissions"] = df["emissions"].round(0)

    # Check if target and historical data is consistent
    df_emissions = df.groupby(["year", "company"])["emissions"].mean().reset_index()

    df = df.merge(
        df_emissions, how="left", on=["year", "company"], suffixes=("", "_mean")
    )

    mismatch = df[df["emissions"] != df["emissions_mean"]].drop(
        columns="emissions_mean"
    )
    if len(mismatch) > 0:
        mismatch.to_csv("mismatch.csv", index=False)
        print("There are some inconsistencies in S1+S2 emission data.")
        print(mismatch)

    df = df.groupby(["year", "company"])["emissions"].mean().round(0).reset_index()

    df_pivot = df.pivot(
        index="year", columns="company", values="emissions"
    ).reset_index()
    df_pivot.to_csv(data_dir("clean", "emission_targets_amended.csv"), index=False)

    # Split into historical part and projection
    df["projection"] = df["year"] > 2022

    last_historical_rows = []
    for company, subdf in df.groupby("company"):
        last_row = subdf[~subdf["projection"]].iloc[-1:, :]
        last_historical_rows.append(last_row)

    df_last_historical = pd.concat(last_historical_rows)
    df_last_historical["projection"] = True

    df = pd.concat([df, df_last_historical]).sort_values("year")

    df["column"] = df["company"]
    df.loc[df["projection"], "column"] += " (proj.)"

    df_pivot = df.pivot(
        index="year", columns="column", values="emissions"
    ).reset_index()

    df_pivot.to_csv(
        data_dir("clean", "emission_targets_amended_and_split.csv"), index=False
    )

    for col in df_pivot.set_index("year").columns:
        if col.endswith("(proj.)"):
            print(col)


if __name__ == "__main__":

    # process_targets()

    append_historical_data_to_emission_targets()
