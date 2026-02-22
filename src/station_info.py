import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import urllib.parse
import re
import time

def get_unique_stations(df: pd.DataFrame) -> np.ndarray:
    """
    データフレームの access_1_station ~ access_3_station から、
    欠損値や空文字を除外したすべてのユニークな駅名の配列を取得する関数。
    """
    # 3つの列の値を一次元配列に展開し、ユニークなものを取得
    cols = [col for col in ['access_1_station', 'access_2_station', 'access_3_station'] if col in df.columns]
    all_stations = pd.unique(df[cols].values.ravel())

    # NaN と 空文字('') を除外
    valid_stations = [st for st in all_stations if not pd.isna(st) and st != '']

    return np.array(valid_stations)

def create_station_time_mapping(unique_stations: np.ndarray, to_station: str = '東京') -> pd.DataFrame:
    """
    ユニークな駅リストを受け取り、Yahoo!乗換案内から指定駅までの所要時間を取得。
    第1ルートの時間と乗り換え回数を取得する。
    """
    station_data = []
    print(f"全 {len(unique_stations)} 駅のアクセス情報取得を開始します！☕️")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for i, st in enumerate(unique_stations, 1):
        station_clean = st.split('/')[-1] if '/' in st else st
        url = f"https://transit.yahoo.co.jp/search/result?from={urllib.parse.quote(station_clean)}&to={urllib.parse.quote(to_station)}"

        time_min = None
        transfer_count = None
        try:
            res = requests.get(url, headers=headers)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # 第1ルートの情報を取得
            route1 = soup.select_one('#route01')
            if route1:
                # 時間の抽出
                time_el = route1.select_one('.time')
                if time_el:
                    text = time_el.get_text()
                    m_hour_min = re.search(r'(\d+)時間(\d+)分', text)
                    m_min = re.search(r'(\d+)分', text)

                    if m_hour_min:
                        time_min = float(int(m_hour_min.group(1)) * 60 + int(m_hour_min.group(2)))
                    elif m_min:
                        time_min = float(m_min.group(1))
                
                # 乗り換え回数の抽出
                transfer_el = route1.select_one('.transfer')
                if transfer_el:
                    transfer_text = transfer_el.get_text()
                    m_transfer = re.search(r'(\d+)回', transfer_text)
                    if m_transfer:
                        transfer_count = float(m_transfer.group(1))
                    elif 'なし' in transfer_text:
                        transfer_count = 0.0
        except Exception as e:
            print(f"Error fetching data for {st}: {e}")

        station_data.append({
            'station_name': st,
            'time_to_target_min': time_min,
            'transfer_count': transfer_count
        })

        # 進捗表示
        if i % 10 == 0 or time_min is None:
            status = f"{time_min}分(乗換{transfer_count}回)" if time_min is not None else "取得失敗"
            print(f"[{i}/{len(unique_stations)}] {st} -> {to_station}駅: {status}")

        time.sleep(1)

    print("\n🎉 全駅の取得が完了しました！")

    return pd.DataFrame(station_data)
