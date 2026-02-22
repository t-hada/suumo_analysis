import marimo

__generated_with = "0.20.1"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import numpy as np
    import os
    import sys
    import marimo as mo

    project_root = "/Users/t-hada/workspace/suumo_analysis"
    src_path = os.path.join(project_root, "src")

    for path in [project_root, src_path]:
        if path not in sys.path:
            sys.path.insert(0, path)

    from analyzer import create_station_rent_summary, calculate_cost_performance
    from visualizer import plot_station_rent_vs_time, plot_cost_performance_ranking

    return (
        calculate_cost_performance,
        create_station_rent_summary,
        mo,
        pd,
        plot_cost_performance_ranking,
        plot_station_rent_vs_time,
    )


@app.cell
def _(create_station_rent_summary, pd):
    df = pd.read_csv(f"data/tokyo_all.csv")
    df_station = create_station_rent_summary(df)
    return (df_station,)


@app.cell
def _(df_station, mo, plot_station_rent_vs_time):
    fig = plot_station_rent_vs_time(df_station, min_properties=5, title="東京駅までの移動時間 vs 駅ごとの平均家賃")
    display_fig = mo.ui.plotly(fig)
    return (display_fig,)


@app.cell
def _(display_fig):
    display_fig
    return


@app.cell
def _(
    calculate_cost_performance,
    df_station,
    mo,
    plot_cost_performance_ranking,
):
    df_ranking = calculate_cost_performance(df_station, min_properties=5)
    fig_rank = plot_cost_performance_ranking(df_ranking, top_n=15)
    display_rank_fig = mo.ui.plotly(fig_rank)
    return (display_rank_fig,)


@app.cell
def _(display_rank_fig):
    display_rank_fig
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
