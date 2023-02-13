import pandas as pd

from temperature_scoring.config import data_dir


def process_targets(suffix=""):

    input_file = data_dir("clean", f"input_data{suffix}.xlsx")
    df = pd.read_excel(input_file, sheet_name="target_data")

    df = df[df["target_type"] == "Absolute"]

    # TODO: aggregate Grupa Azoty targets
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
