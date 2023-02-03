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


def calculate_score(
    suffix="", revised_combined_score=False, aggregation_methods=["WATS"]
):
    input_file = data_dir("clean", f"input_data{suffix}.xlsx")
    output_file = data_dir("clean", f"output_data{suffix}.xlsx")

    time_frames = [ETimeFrames.SHORT, ETimeFrames.MID, ETimeFrames.LONG]
    scopes = [EScope.S1S2, EScope.S1S2S3, EScope.S3]

    temperature_score = TemperatureScore(
        time_frames=time_frames,
        scopes=scopes,
    )

    data_provider = ExcelProvider(input_file)
    df_portfolio = pd.read_excel(input_file, sheet_name="portfolio_data")
    portfolio = SBTi.utils.dataframe_to_portfolio(df_portfolio)

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
    if revised_combined_score:
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
        df["scope"] = df["scope"].replace(
            {"S1S2": EScope.S1S2, "S3": EScope.S3, "S1S2S3": EScope.S1S2S3}
        )

    df.to_excel(output_file, index=False)

    # Aggregate scores
    if revised_combined_score:
        df["temperature_score"] = df["revised_temperature_score"].fillna(
            df["temperature_score"]
        )
    for method in aggregation_methods:
        # aggregate by assets
        temperature_score.aggregation_method = {
            "WATS": PortfolioAggregationMethod.WATS,
            "AOTS": PortfolioAggregationMethod.AOTS,
            "ROTS": PortfolioAggregationMethod.ROTS,
        }[method]
        # aggregate by revenues
        aggregated_scores = temperature_score.aggregate_scores(df)
        df_agg = pd.DataFrame(aggregated_scores.dict()).applymap(
            lambda x: round(x["all"]["score"], 2)
        )
        df_agg.index.name = "scope"
        df_agg.to_csv(data_dir("clean", f"portfolio_scores{suffix}_{method}.csv"))


if __name__ == "__main__":

    calculate_score("_example")
    calculate_score("", revised_combined_score=True)
    calculate_score(
        "_with_estimates",
        revised_combined_score=False,
        aggregation_methods=["WATS", "AOTS", "ROTS"],
    )
