import pandas as pd

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

    df.to_excel(output_file, index=False)


if __name__ == "__main__":

    calculate_score()
    calculate_score("_example")
