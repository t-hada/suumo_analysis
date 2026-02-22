import plotly.express as px
import pandas as pd

def plot_station_rent_vs_time(df_summary: pd.DataFrame, min_properties: int = 10, title: str = '東京駅までの移動時間 vs 駅ごとの平均家賃'):
    """
    平均家賃と移動時間の散布図をプロットする。
    """
    df_plot = df_summary[df_summary['property_count'] >= min_properties].copy()
    df_plot['mean_rent_man'] = (df_plot['mean_rent'] / 10000).round(1)

    fig = px.scatter(
        df_plot,
        x='time_to_tokyo_min', 
        y='mean_rent',
        hover_name='station_name',
        hover_data={
            'mean_rent': ':.0f',
            'mean_rent_man': True,
            'time_to_tokyo_min': True,
            'property_count': True
        },
        size='property_count',
        color='mean_rent',
        color_continuous_scale='Portland',
        title=title,
        labels={
            'time_to_tokyo_min': '移動時間（分）',
            'mean_rent': '平均総家賃（円）',
            'mean_rent_man': '平均総家賃（万円）',
            'property_count': '物件掲載数'
        },
        opacity=0.8,
        template='plotly_white'
    )

    fig.update_layout(
        xaxis=dict(range=[0, 90], dtick=10),
        yaxis=dict(range=[0, 300000], rangemode='tozero', tickformat=','),
        font=dict(family="Meiryo, sans-serif")
    )

    return fig

def plot_cost_performance_ranking(df_ranking: pd.DataFrame, top_n: int = 20):
    """
    コスパ最強駅ランキングを横向き棒グラフで可視化する。
    """
    top_df_for_plot = df_ranking.head(top_n).sort_values('bargain_amount', ascending=True)

    fig = px.bar(
        top_df_for_plot,
        x='bargain_man',
        y='station_name',
        orientation='h',
        title=f'🔥 コスパ最強駅ランキング TOP{top_n}（相場からの割安額）',
        labels={
            'bargain_man': '相場より安い額（万円）',
            'station_name': '駅名'
        },
        hover_data={
            'time_to_tokyo_min': True,
            'mean_rent_man': True,
            'property_count': True
        },
        color='bargain_man',
        color_continuous_scale='Viridis',
        text='bargain_man',
        template='plotly_white'
    )

    fig.update_traces(textposition='outside')
    fig.update_layout(font=dict(family="Meiryo, sans-serif"))

    return fig
