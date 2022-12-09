import pandas as pd

import SBTi
from SBTi.data.excel import ExcelProvider
from SBTi.portfolio_aggregation import PortfolioAggregationMethod
from SBTi.portfolio_coverage_tvp import PortfolioCoverageTVP
from SBTi.temperature_score import TemperatureScore, Scenario
from SBTi.target_validation import TargetProtocol
from SBTi.interfaces import ETimeFrames, EScope

from temperature_scoring.config import data_dir

if __name__ == "__main__":

    input_file = data_dir("clean", "input_data.xlsx")
    output_file = data_dir("clean", "output_data.xlsx")

    provider = ExcelProvider(input_file)
    provider.data = pd.read_excel(
        input_file,
        sheet_name=None,
    )

    temperature_score = TemperatureScore(  # all available options:
        time_frames=list(
            SBTi.interfaces.ETimeFrames
        ),  # ETimeFrames: SHORT MID and LONG
        scopes=[EScope.S1S2, EScope.S3, EScope.S1S2S3],  # EScopes: S3, S1S2 and S1S2S3
        aggregation_method=PortfolioAggregationMethod.WATS,  # Options for the aggregation method are WATS, TETS, AOTS, MOTS, EOTS, ECOTS, and ROTS.
    )

    df_portfolio = pd.read_excel(input_file, sheet_name="portfolio_data")
    companies = SBTi.utils.dataframe_to_portfolio(df_portfolio)

    amended_portfolio = temperature_score.calculate(
        data_providers=[provider], portfolio=companies
    )

    print(amended_portfolio.head(15))

    amended_portfolio.to_excel(output_file, index=False)
