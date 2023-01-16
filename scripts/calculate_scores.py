import pandas as pd
import numpy as np

import SBTi
from SBTi.data.excel import ExcelProvider
from SBTi.portfolio_aggregation import PortfolioAggregationMethod
from SBTi.portfolio_coverage_tvp import PortfolioCoverageTVP
from SBTi.temperature_score import TemperatureScore, Scenario
from SBTi.target_validation import TargetProtocol
from SBTi.interfaces import ETimeFrames, EScope

from temperature_scoring.config import data_dir


def calculate_score(suffix=""):
    input_file = data_dir("clean", f"input_data{suffix}.xlsx")
    output_file = data_dir("clean", f"output_data{suffix}.xlsx")

    time_frames = [ETimeFrames.SHORT, ETimeFrames.MID, ETimeFrames.LONG]
    scopes = [EScope.S1S2, EScope.S1S2S3, EScope.S3]

    temperature_score = TemperatureScore(
        time_frames=time_frames,
        scopes=scopes,
    )

    data_provider = ExcelProvider(input_file)
    portfolio = SBTi.utils.dataframe_to_portfolio(
        pd.read_excel(input_file, sheet_name="portfolio_data")
    )

    df = temperature_score.calculate(
        data_providers=[data_provider],
        portfolio=portfolio,
    )

    df["target_type"] = df["target_type"].str.capitalize()

    # Keep only non-default scores
    # df = df[df["temperature_results"] < 1]

    columns = ["company_name", "company_id"]
    columns = columns + [col for col in df.columns if col not in columns]
    df = df[columns]

    # Revise S1+S2+S3 scores if GHG emissions data are missing
    df["scope"] = df["scope"].astype(str)
    df_revised = (
        df[
            [
                "company_id",
                "time_frame",
                "ghg_s1s2",
                "ghg_s3",
                "scope",
                "temperature_score",
            ]
        ]
        .pivot(
            index=["company_id", "time_frame", "ghg_s1s2", "ghg_s3"],
            columns="scope",
            values="temperature_score",
        )
        .reset_index()
    )
    missing_ghg = df_revised["ghg_s1s2"].isna() | df_revised["ghg_s3"].isna()
    df_revised.loc[missing_ghg, "S1S2S3"] = df_revised.loc[
        missing_ghg, ["S1S2", "S3"]
    ].max(axis=1)
    df_revised = df_revised.melt(
        id_vars=["company_id", "time_frame"],
        value_vars=["S1S2", "S3", "S1S2S3"],
        var_name="scope",
        value_name="revised_temperature_score",
    )
    df = df.merge(df_revised, on=["company_id", "time_frame", "scope"], how="left")

    df.to_excel(output_file, index=False)


if __name__ == "__main__":

    calculate_score()
    calculate_score("_example")
