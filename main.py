import os
import pandas as pd
from src.scraper import get_suumo_data, save_csv
from src.cleaner import clean_suumo_data
from src.station_info import get_unique_stations, create_station_time_mapping
from src.analyzer import merge_times_to_main_df

def process_suumo_pipeline(url, name, end_page, start_page=1):
    """
    1. スクレイピング (Raw CSV保存)
    2. クリーニング (路線/駅分割、数値化)
    3. 駅名抽出 & 電車所要時間取得 (Times CSV保存)
    4. マージ & 最終クリーンCSV保存
    """
    os.makedirs("data", exist_ok=True)

    # --- 1. スクレイピング ---
    print(f"\n--- Starting: {name} ---")
    data_samples = get_suumo_data(url, end_page, start_page)
    raw_csv_path = save_csv(data_samples, "data", f"{name}_suumo")
    print(f"Raw data saved to: {raw_csv_path}")

    # --- 2. クリーニング ---
    print(f"Cleaning data and splitting lines/stations...")
    df_raw = pd.read_csv(raw_csv_path)
    df_clean = clean_suumo_data(df_raw)

    # --- 3. 駅情報の取得 (所要時間) ---
    station_times_path = f"data/{name}_station.csv"
    if os.path.exists(station_times_path):
        print(f"Loading existing station times from {station_times_path}")
        df_times = pd.read_csv(station_times_path)
    else:
        df_times = pd.DataFrame(columns=['station_name', 'time_to_target_min', 'transfer_count'])

    unique_stations = get_unique_stations(df_clean)
    
    # 既存のCSVにない駅のみ取得
    existing_stations = set(df_times['station_name'].dropna().unique())
    new_stations = [s for s in unique_stations if s not in existing_stations]

    if new_stations:
        print(f"Fetching transit times for {len(new_stations)} new stations...")
        df_new_times = create_station_time_mapping(new_stations)
        # 結合して保存
        df_times = pd.concat([df_times, df_new_times], ignore_index=True).drop_duplicates(subset=['station_name'])
        df_times.to_csv(station_times_path, index=False, encoding="utf-8-sig")
        print(f"Updated station times saved to: {station_times_path}")
    else:
        print("All stations already exist in the master list.")

    # --- 4. マージ & 最終保存 ---
    print("Merging transit times into cleaned data...")
    df_final = merge_times_to_main_df(df_clean, df_times)
    
    # カラムをご指定の順番に並び替え
    column_order = [
        'building_name', 'category', 'address', 'layout', 'area', 'floor', 'stories', 'age', 
        'rent', 'admin_fee', 'deposit', 'gratuity',
        'access_1_line', 'access_1_station', 'access_1_walk_min', 'access_1_time_min', 'access_1_transfer_count',
        'access_2_line', 'access_2_station', 'access_2_walk_min', 'access_2_time_min', 'access_2_transfer_count',
        'access_3_line', 'access_3_station', 'access_3_walk_min', 'access_3_time_min', 'access_3_transfer_count',
        'url', 'acquired_at'
    ]
    
    # 存在するカラムのみで並び替え（エラー防止）
    df_final = df_final[[col for col in column_order if col in df_final.columns]]
    
    final_csv_path = f"data/{name}.csv"
    df_final.to_csv(final_csv_path, index=False, encoding="utf-8-sig")
    print(f"Done! Final cleaned CSV: {final_csv_path}")

def main():
    end_page = 10
    # 取得したいURLと名前のリスト
    tasks = [
        {
            "name": "tokyo_all",
            "url": "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&ta=13&bs=040&ekInput=25620&tj=90&nk=-1&ct=9999999&cb=0.0&et=9999999&mt=9999999&mb=0&cn=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&pc=30&page={}",
            "end_page": end_page
        },
        {
            "name": "tokyo_rebar",
            "url": "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&ta=13&bs=040&ekInput=25620&tj=90&nk=-1&ct=9999999&cb=0.0&kz=1&et=9999999&mt=9999999&mb=0&cn=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&pc=30&page={}",
            "end_page": end_page
        },
        {
            "name": "tokyo_steel",
            "url": "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&ta=13&bs=040&ekInput=25620&tj=90&nk=-1&ct=9999999&cb=0.0&kz=1&et=9999999&mt=9999999&mb=0&cn=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&pc=30&page={}",
            "end_page": end_page
        },
        {
            "name": "tokyo_wooden",
            "url": "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&ta=13&bs=040&ekInput=25620&tj=90&nk=-1&ct=9999999&cb=0.0&kz=3&et=9999999&mt=9999999&mb=0&cn=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&pc=30&page={}",
            "end_page": end_page
        },
        {
            "name": "tokyo_indoor_washing_machine",
            "url": "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&ta=13&bs=040&ekInput=25620&tj=90&nk=-1&ct=9999999&cb=0.0&et=9999999&mt=9999999&mb=0&cn=9999999&tc=0400501&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&pc=30&page={}",
            "end_page": end_page
        },
        {
            "name": "tokyo_separate_washroom",
            "url": "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&ta=13&bs=040&ekInput=25620&tj=90&nk=-1&ct=9999999&cb=0.0&et=9999999&mt=9999999&mb=0&cn=9999999&tc=0400502&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&pc=30&page={}",
            "end_page": end_page
        },
        {
            "name": "tokyo_soundproof",
            "url": "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&ta=13&bs=040&ekInput=25620&tj=90&nk=-1&ct=9999999&cb=0.0&et=9999999&mt=9999999&mb=0&cn=9999999&tc=0400506&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&pc=30&page={}",
            "end_page": end_page
        },
        {
            "name": "tokyo_separate_bath_and_toilet",
            "url": "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&ta=13&bs=040&ekInput=25620&tj=90&nk=-1&ct=9999999&cb=0.0&et=9999999&mt=9999999&mb=0&cn=9999999&tc=0400301&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&pc=30&page={}",
            "end_page": end_page
        },
        {
            "name": "tokyo_washlet",
            "url": "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&ta=13&bs=040&ekInput=25620&tj=90&nk=-1&ct=9999999&cb=0.0&et=9999999&mt=9999999&mb=0&cn=9999999&tc=0400302&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&pc=30&page={}",
            "end_page": end_page
        },
        {
            "name": "tokyo_bathroom_Dryer",
            "url": "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&ta=13&bs=040&ekInput=25620&tj=90&nk=-1&ct=9999999&cb=0.0&et=9999999&mt=9999999&mb=0&cn=9999999&tc=0400303&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&pc=30&page={}",
            "end_page": end_page
        },
    ]

    for task in tasks:
        process_suumo_pipeline(task["url"], task["name"], task["end_page"])

if __name__ == "__main__":
    main()
