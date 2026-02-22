import requests
from bs4 import BeautifulSoup
from retry import retry
import urllib.parse
import time
import pandas as pd
import os
import datetime

@retry(tries=3, delay=10, backoff=2)
def load_page(url):
    html = requests.get(url)
    soup = BeautifulSoup(html.content, 'html.parser')
    return soup

def get_suumo_data(base_url, max_page=10, start_page=1):
    data_samples = []

    for page in range(start_page, max_page + 1):
        soup = load_page(base_url.format(page))
        mother = soup.find_all(class_='cassetteitem')

        for child in mother:
            data_home = []
            # カテゴリ
            data_home.append(child.find(class_='ui-pct ui-pct--util1').text)
            # 建物名
            data_home.append(child.find(class_='cassetteitem_content-title').text)
            # 住所
            data_home.append(child.find(class_='cassetteitem_detail-col1').text)
            
            # 最寄り駅のアクセス (常に3つの要素を確保)
            access_elements = child.find(class_='cassetteitem_detail-col2').find_all(class_='cassetteitem_detail-text')
            for i in range(3):
                if i < len(access_elements):
                    data_home.append(access_elements[i].text)
                else:
                    data_home.append("")
                    
            # 築年数と階数 (常に2つの要素を確保)
            age_stories_elements = child.find(class_='cassetteitem_detail-col3').find_all('div')
            for i in range(2):
                if i < len(age_stories_elements):
                    data_home.append(age_stories_elements[i].text)
                else:
                    data_home.append("")

            # 部屋情報
            rooms = child.find(class_='cassetteitem_other')
            for room in rooms.find_all(class_='js-cassette_link'):
                data_room = []
                
                # 部屋情報が入っている表を探索
                for id_, grandchild in enumerate(room.find_all('td')):
                    # 階
                    if id_ == 2:
                        data_room.append(grandchild.text.strip())
                    # 家賃と管理費
                    elif id_ == 3:
                        data_room.append(grandchild.find(class_='cassetteitem_other-emphasis ui-text--bold').text)
                        data_room.append(grandchild.find(class_='cassetteitem_price cassetteitem_price--administration').text)
                    # 敷金と礼金
                    elif id_ == 4:
                        data_room.append(grandchild.find(class_='cassetteitem_price cassetteitem_price--deposit').text)
                        data_room.append(grandchild.find(class_='cassetteitem_price cassetteitem_price--gratuity').text)
                    # 間取りと面積
                    elif id_ == 5:
                        data_room.append(grandchild.find(class_='cassetteitem_madori').text)
                        data_room.append(grandchild.find(class_='cassetteitem_menseki').text)
                    # url
                    elif id_ == 8:
                        get_url = grandchild.find(class_='js-cassette_link_href cassetteitem_other-linktext').get('href')
                        abs_url = urllib.parse.urljoin(base_url, get_url)
                        data_room.append(abs_url)
                
                # 取得時間の追加
                acquired_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 物件情報 + 部屋情報 + 取得時間
                data_sample = data_home + data_room + [acquired_at]
                data_samples.append(data_sample)
        
        time.sleep(1)
        print(f'{page}ページ目：{len(data_samples)}件取得 Done!', flush=True)

    return data_samples

def save_csv(data_samples, save_dir, name):
    columns = [
        'category', 'building_name', 'address', 'access_1', 'access_2', 'access_3',
        'age', 'stories', 'floor', 'rent', 'admin_fee', 'deposit', 'gratuity',
        'layout', 'area', 'url', 'acquired_at'
    ]
    file_path = os.path.join(save_dir, f'{name}.csv')
    df = pd.DataFrame(data_samples, columns=columns)
    df.to_csv(file_path, index=False, encoding='utf-8-sig')
    return file_path
