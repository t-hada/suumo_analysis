import pandas as pd
import numpy as np

def clean_suumo_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """SUUMOのスクレイピングデータを分析用に整形する関数"""
    df = df_raw.copy()

    # 1. アクセス情報の分割
    for i in range(1, 4):
        col = f'access_{i}'
        if col not in df.columns:
            continue

        # 文字列に変換し、欠損値を空文字に
        s = df[col].astype(str).replace('nan', '')

        # 「歩〇〇分」または「車〇〇分」などの前の部分を取得
        # 例: "ＪＲ中央線/立川駅 歩5分" -> "ＪＲ中央線/立川駅"
        line_station_part = s.str.split(' ').str[0]
        
        # 路線と駅を分割 (例: "ＪＲ中央線/立川駅" -> ["ＪＲ中央線", "立川駅"])
        # "/" がある場合は分割、ない場合は路線をNaN、駅をそのままの値にする
        split_data = line_station_part.str.split('/', n=1)
        
        # 路線: 分割の結果、要素が2つあれば最初の要素、1つならNaN
        df[f'access_{i}_line'] = split_data.apply(lambda x: x[0] if isinstance(x, list) and len(x) > 1 else np.nan)
        # 駅: 分割の結果、要素が2つあれば2番目の要素、1つならその要素自身
        df[f'access_{i}_station'] = split_data.apply(lambda x: x[-1] if isinstance(x, list) else np.nan)
        
        # 「歩〇〇分」から数字だけを抽出
        df[f'access_{i}_walk_min'] = s.str.extract(r'歩(\d+)分')[0].astype(float)

        # 元の列を削除
        df = df.drop(columns=[col])

    # 2. 金額系データの数値化 (万円と円をすべて「円」に統一)
    money_cols = ['rent', 'admin_fee', 'deposit', 'gratuity']
    for col in money_cols:
        if col not in df.columns:
            continue

        # 文字列化し、ハイフンなどを0にする
        s = df[col].astype(str).replace(['-', 'nan', ' ', ''], '0')

        # '万円'が含まれる行を判定
        is_man = s.str.contains('万円', na=False)

        # 数字部分だけを抽出してfloat型に
        nums = s.str.extract(r'([\d\.]+)')[0].astype(float).fillna(0.0)

        # '万円'がついていた行は10000倍、それ以外はそのまま
        df[col] = np.where(is_man, nums * 10000, nums)

    # 3. 築年数
    if 'age' in df.columns:
        s_age = df['age'].astype(str)
        # 先に数値を抽出して欠損値を0にしておく
        age_nums = s_age.str.extract(r'(\d+)')[0].astype(float).fillna(0.0)
        # 新築なら0、そうでなければ抽出した数値
        df['age'] = np.where(s_age.str.contains('新築'), 0.0, age_nums)

    # 4. 階建
    if 'stories' in df.columns:
        df['stories'] = df['stories'].astype(str).str.extract(r'(\d+)階建')[0].astype(float)

    # 5. 階数 (地下の場合はマイナス)
    if 'floor' in df.columns:
        s_floor = df['floor'].astype(str).replace(['-', 'nan', ''], '0')
        # 数字を抽出
        floor_nums = s_floor.str.extract(r'(\d+)')[0].astype(float)
        # 地下・Bを含む場合はマイナスにする
        is_underground = s_floor.str.contains('地下|B', na=False)
        df['floor'] = np.where(is_underground, -floor_nums, floor_nums)

    # 6. 面積
    if 'area' in df.columns:
        df['area'] = df['area'].astype(str).str.replace('m2', '', regex=False).replace('-', '0').astype(float)

    return df
