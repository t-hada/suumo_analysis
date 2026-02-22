import pandas as pd
import numpy as np

def merge_times_to_main_df(df: pd.DataFrame, df_times: pd.DataFrame, station_col_in_times: str = 'station_name', time_col: str = 'time_to_target_min', transfer_col: str = 'transfer_count') -> pd.DataFrame:
    """
    大元の物件データ(df)に、対応表(df_times)の電車時間と乗り換え回数をマッピングする。
    """
    df_result = df.copy()

    # 対応表を辞書型に変換
    time_mapping = dict(zip(df_times[station_col_in_times], df_times[time_col]))
    transfer_mapping = dict(zip(df_times[station_col_in_times], df_times[transfer_col]))

    # 各アクセス駅のカラムに対して、マッピング
    for i in range(1, 4):
        station_col = f'access_{i}_station'
        new_time_col = f'access_{i}_time_min'
        new_transfer_col = f'access_{i}_transfer_count'

        if station_col in df_result.columns:
            df_result[new_time_col] = df_result[station_col].map(time_mapping)
            df_result[new_transfer_col] = df_result[station_col].map(transfer_mapping)

    return df_result

def create_station_rent_summary(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    物件データから「駅ごとの平均家賃」を計算し、「電車時間」と結合してサマリーを作成する。
    """
    df = df_merged.copy()

    # 1. 総家賃を計算 (家賃 + 管理費)
    df['total_rent'] = df['rent'].fillna(0) + df['admin_fee'].fillna(0)

    # 2. access_1 ~ 3 を縦に展開
    melted_df = pd.melt(
        df,
        id_vars=['total_rent'],
        value_vars=['access_1_station', 'access_2_station', 'access_3_station'],
        value_name='station_name'
    )

    # 空データを削除
    melted_df = melted_df.dropna(subset=['station_name'])
    melted_df = melted_df[melted_df['station_name'] != '']

    # 3. 駅ごとに平均家賃と物件数を集計
    summary_df = melted_df.groupby('station_name').agg(
        mean_rent=('total_rent', 'mean'),
        property_count=('total_rent', 'count')
    ).reset_index()

    # 4. 時間データを再度マッピング
    times_melted = pd.melt(
        df,
        value_vars=['access_1_station', 'access_2_station', 'access_3_station'],
        value_name='station_name'
    )
    times_melted['time'] = pd.melt(
        df,
        value_vars=['access_1_time_min', 'access_2_time_min', 'access_3_time_min'],
        value_name='time'
    )['time']

    times_melted = times_melted.dropna(subset=['station_name', 'time'])
    
    # 駅名ごとに時間の平均をとる
    station_times = times_melted.groupby('station_name')['time'].mean().reset_index()
    station_times.rename(columns={'time': 'time_to_tokyo_min'}, inplace=True)

    # 5. 平均家賃と電車時間を結合
    final_summary = pd.merge(summary_df, station_times, on='station_name', how='inner')

    return final_summary

def calculate_cost_performance(df_summary: pd.DataFrame, min_properties: int = 10) -> pd.DataFrame:
    """
    回帰分析を用いて「相場（トレンドライン）からの割安度」を計算する。
    """
    # 1. 信頼性の高いデータに絞る
    df_eval = df_summary[df_summary['property_count'] >= min_properties].copy()
    if df_eval.empty:
        return df_eval

    # 2. 回帰分析 (1次式: y = ax + b)
    x = df_eval['time_to_tokyo_min']
    y = df_eval['mean_rent']
    coef = np.polyfit(x, y, 1)
    poly_1d = np.poly1d(coef)

    # 3. 適正家賃（予測値）と割安度を計算
    df_eval['predicted_rent'] = poly_1d(x)
    df_eval['bargain_amount'] = df_eval['predicted_rent'] - df_eval['mean_rent']
    
    # 万円単位も追加
    df_eval['bargain_man'] = (df_eval['bargain_amount'] / 10000).round(2)
    df_eval['mean_rent_man'] = (df_eval['mean_rent'] / 10000).round(1)

    # お得な順に並び替え
    df_ranking = df_eval.sort_values('bargain_amount', ascending=False).reset_index(drop=True)

    return df_ranking
