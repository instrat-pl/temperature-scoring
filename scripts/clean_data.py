import pandas as pd

from temperature_scoring.config import data_dir


if __name__ == "__main__":

    dfs = pd.read_excel(
        data_dir("raw", "Dane - spółki z ogłoszonymi celami .xlsx"), sheet_name=None
    )

    new_names = {
        "Target Data": "target_data",
        "Fundamental Data": "fundamental_data",
        "Portfolio Data": "portfolio_data",
    }

    with pd.ExcelWriter(data_dir("clean", "input_data.xlsx")) as writer:
        for name, df in dfs.items():
            df.to_excel(writer, sheet_name=new_names[name], index=False)
