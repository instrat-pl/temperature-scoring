import os
import pandas as pd
from itertools import product

import plotly.express as px
import plotly.graph_objs as go
import plotly.io as pio

from temperature_scoring.config import data_dir, plots_dir


def make_instrat_template():
    # template = go.layout.Template()
    template = pio.templates[pio.templates.default]
    template.layout.title.font = dict(
        family="Work Sans Medium, sans-serif", size=18, color="black"
    )
    template.layout.font = dict(
        family="Work Sans Light, sans-serif", size=13, color="black"
    )
    template.layout.colorway = [
        "#000000",
        "#c0843f",
        "#5f468f",
        "#1d6995",
        "#38a5a4",
        "#0f8454",
        "#73ae48",
        "#ecac08",
        "#e07c05",
        "#cb503e",
        "#93346e",
        "#6f4070",
        "#666666",
    ]
    return template


def plot_heatmap(df):

    template = make_instrat_template()
    width = (len(df.columns) + 8) * 50
    height = (len(df.index) + 2) * 50

    fig = px.imshow(
        df,
        text_auto=True,
        labels={"color": "Temperature Score"},
        color_continuous_scale="OrRd",
        zmin=0,
        zmax=4,
        template=template,
        width=width,
        height=height,
    )
    fig.update_xaxes(side="top")
    fig.update_layout(
        coloraxis={
            "colorbar": {
                "lenmode": "pixels",
                "len": 0.5 * height,
                "ypad": 0,
                "x": 1.0,
                "y": 0.55,
                "ticklabelposition": "outside left",
            }
        }
    )

    return fig


def plot_temperature_scores(
    suffix="", use_revised_scores=False, aggregation_methods=["Average"]
):
    output_file = data_dir("clean", f"output_data{suffix}.xlsx")
    plot_file = plots_dir(f"temperature_scores{suffix}.png")
    plot_data_file = plots_dir(f"temperature_scores{suffix}.csv")

    df = pd.read_excel(output_file)

    df["time_frame"] = df["time_frame"].str.capitalize()
    df["scope"] = df["scope"].replace({"S1S2": "S1+S2", "S1S2S3": "S1+S2+S3"})
    df["Scope and Time Frame"] = df["scope"] + " " + df["time_frame"]
    df = df.rename(
        columns={
            "company_name": "Company",
            "temperature_score"
            if not use_revised_scores
            else "revised_temperature_score": "Temperature Score",
        }
    )
    df = df.pivot(
        index="Company", columns="Scope and Time Frame", values="Temperature Score"
    )

    # Sort companies alphabetically
    df = df.sort_index(key=lambda x: x.str.lower())

    # Sort columns first by scope, then by time frame
    scopes = ["S1+S2", "S3", "S1+S2+S3"]
    time_frames = ["Short", "Mid", "Long"]
    columns = [" ".join(p) for p in product(scopes, time_frames)]
    df = df[[col for col in columns if col in df.columns]]

    for method in aggregation_methods:
        df_agg = pd.read_csv(
            data_dir("clean", f"portfolio_scores{suffix}_{method}.csv")
        )
        df_agg["scope"] = df_agg["scope"].replace(
            {"S1S2": "S1+S2", "S1S2S3": "S1+S2+S3"}
        )
        df_agg = df_agg.melt(
            id_vars="scope", var_name="time_frame", value_name="Temperature Score"
        )
        df_agg["time_frame"] = df_agg["time_frame"].str.capitalize()
        df_agg["Scope and Time Frame"] = df_agg["scope"] + " " + df_agg["time_frame"]
        df_agg["Company"] = method
        df_agg = df_agg.pivot(
            index="Company", columns="Scope and Time Frame", values="Temperature Score"
        )
        df = pd.concat([df, df_agg])

    df.to_csv(plot_data_file)
    fig = plot_heatmap(df)

    fig.write_image(plot_file, scale=2)


if __name__ == "__main__":

    os.makedirs(plots_dir(), exist_ok=True)

    # plot_temperature_scores("_example")
    plot_temperature_scores(use_revised_scores=True, aggregation_methods=["Average"])
    plot_temperature_scores("_with_estimates", aggregation_methods=["Average", "Emissions", "Revenue", "Market Cap"])
