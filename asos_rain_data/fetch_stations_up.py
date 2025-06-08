import os
import numpy as np
import math
import json
import requests
import time
from urllib.parse import quote_plus, urlencode
from datetime import datetime, timedelta
import pandas as pd
import tqdm

times = datetime.today() - timedelta(days=1)
today = times.strftime("%m%d")


def dw_weather_multiple(stations, start, end, output_path):
    url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'
    servicekey = 'HOhrXN4295f2VXKpOJc4gvpLkBPC/i97uWk8PfrUIONlI7vRB9ij088/F5RvIjZSz/PUFjJ4zkMjuBkbtMHqUg=='
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}

    current_year = datetime.today().year
    current_date = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

    for index, station in stations.iterrows():
        stn_id = station['지점코드']
        latitude = station['위도']
        longitude = station['경도']
        altitude = station['고도']
        station_name = station['지점명']

        # 폴더 경로 생성(cache_weather > stn_id)
        cache_dir = os.path.join(output_path, "cache_weather", str(stn_id))
        os.makedirs(cache_dir, exist_ok=True)

        all_years_dfs = []

        for y in tqdm.tqdm(range(start, end + 1), desc=f"Downloading {station_name} ({stn_id})"):
            end_date = current_date if y == current_year else f"{y}1231"

            cache_filename = os.path.join(cache_dir, f"{stn_id}_{station_name}_{y}.csv")

            # 이미 캐시된 파일이 있는지 확인
            if os.path.exists(cache_filename):
                print(f"Data for {station_name} ({stn_id}) in {y} already exists. Skipping.")
                continue

            params = f'?{quote_plus("ServiceKey")}={servicekey}&' + urlencode({
                quote_plus("pageNo"): "1",
                quote_plus("numOfRows"): "720",
                quote_plus("dataType"): "JSON",
                quote_plus("dataCd"): "ASOS",
                quote_plus("dateCd"): "DAY",
                quote_plus("startDt"): f"{y}0101",
                quote_plus("endDt"): end_date,
                quote_plus("stnIds"): f"{stn_id}"
            })

            try:
                result = requests.get(url + params, headers=headers)
            except:
                time.sleep(2)
                result = requests.get(url + params, headers=headers)

            try:
                js = json.loads(result.content)
                weather = pd.DataFrame(js['response']['body']['items']['item'])
            except:
                print(f"Failed to fetch data for {station_name} ({stn_id}) in {y}")
                continue

            # 먼저 날짜 관련 컬럼들을 생성 (안전하고 효율적인 방법)
            ts = pd.to_datetime(weather['tm'], errors='coerce')
            weather['year'] = ts.dt.year
            weather['month'] = ts.dt.month
            weather['day'] = ts.dt.day
            weather['date'] = ts
            weather['doy'] = ts.dt.dayofyear

            # 필요한 컬럼만 선택 (습도 포함)
            li = ['year', 'month', 'day', 'doy', 'date',
                  'sumGsr', 'maxTa', 'minTa', 'avgTa', 'avgRhm',
                  'sumRn', 'sumSsHr', 'avgWs']
            weather = weather.loc[:, li]

            # 숫자 컬럼만 변환 (날짜 관련 컬럼들은 이미 변환됨)
            numeric_cols = ['sumGsr', 'maxTa', 'minTa', 'avgTa', 'avgRhm', 'sumRn', 'sumSsHr', 'avgWs']
            weather[numeric_cols] = weather[numeric_cols].apply(pd.to_numeric, errors='coerce')

            list_dfs = [weather]

            # Concatenate and save
            if list_dfs:
                df = pd.concat(list_dfs)
                df.columns = ['year', 'month', 'day', 'doy', 'date', 'radn', 'maxt', 'mint', 'tavg', 'humid', 'rain',
                              'sunhours', 'wind']

                # 필요한 컬럼만 선택
                df = df[
                    ['year', 'month', 'day', 'doy', 'date', 'radn', 'maxt', 'mint', 'tavg', 'humid', 'rain', 'sunhours',
                     'wind']]

                df['rain'] = df['rain'].fillna(0)
                df['latitude'] = latitude
                df['longitude'] = longitude
                df['altitude'] = altitude

                # 간단한 보간만 적용
                df = interpolate_weather(df)

                all_years_dfs.append(df)
                print(f"Saved data for station {station_name} ({stn_id}) to {cache_filename}.")

        # (연도별 for‐loop가 끝난 직후) 월별·주차별 집계 및 저장
        if all_years_dfs:
            # 1) 모든 연도 일별 데이터 합치기
            station_df = pd.concat(all_years_dfs, ignore_index=True)

            # date 컬럼은 이미 존재하므로 중복 생성 불필요
            station_df['week'] = station_df['date'].dt.isocalendar().week

            # 2) 월별 집계 (year, month 기준) - 습도 컬럼 추가
            monthly = station_df.groupby(['year', 'month']).agg(
                # rain_days_gt0=('rain', lambda s: (s > 0).sum()),  # 총_강우횟수 (강수일수, 0mm 초과)
                # rain_days_10_to_30=('rain', lambda s: ((s >= 10) & (s < 30)).sum()),  # 강우_10mm이상_30mm미만
                # rain_days_30_to_50=('rain', lambda s: ((s >= 30) & (s < 50)).sum()),  # 강우_30mm이상_50mm미만
                # rain_days_50_to_70=('rain', lambda s: ((s >= 50) & (s < 70)).sum()),  # 강우_50mm이상_70mm미만
                # rain_days_70_to_90=('rain', lambda s: ((s >= 70) & (s < 90)).sum()),  # 강우_70mm이상_90mm미만
                # rain_days_90_to_100=('rain', lambda s: ((s >= 90) & (s < 100)).sum()),  # 강우_90mm이상_100mm미만
                # rain_days_gt100=('rain', lambda s: (s >= 100).sum()),  # 강우_100mm이상

                # total_rain=('rain', 'sum'),  # 강우량합계
                total_radn=('radn', 'sum'),  # 일사량합계
                # avg_maxt=('maxt', 'mean'),  # 최고기온평균
                # avg_mint=('mint', 'mean'),  # 최저기온평균
                # avg_tavg=('tavg', 'mean'),  # 평균기온평균
                # avg_humid=('humid', 'mean'),  # 평균습도
                # total_sunhours=('sunhours', 'sum'),  # 일조시간합계
                # avg_wind=('wind', 'mean')  # 평균풍속
            ).reset_index()

            weekly = station_df.groupby(['year', 'week']).agg(
                rain_days_gt0=('rain', lambda s: (s > 0).sum()),  # 총_강우횟수 (강수일수, 0mm 초과)
                rain_days_10_to_30=('rain', lambda s: ((s >= 10) & (s < 30)).sum()),  # 강우_10mm이상_30mm미만
                rain_days_30_to_50=('rain', lambda s: ((s >= 30) & (s < 50)).sum()),  # 강우_30mm이상_50mm미만
                rain_days_50_to_70=('rain', lambda s: ((s >= 50) & (s < 70)).sum()),  # 강우_50mm이상_70mm미만
                rain_days_70_to_90=('rain', lambda s: ((s >= 70) & (s < 90)).sum()),  # 강우_70mm이상_90mm미만
                rain_days_90_to_100=('rain', lambda s: ((s >= 90) & (s < 100)).sum()),  # 강우_90mm이상_100mm미만
                rain_days_gt100=('rain', lambda s: (s >= 100).sum()),  # 강우_100mm이상

                total_rain=('rain', 'sum'),  # 강우량합계
                total_radn=('radn', 'sum'),  # 일사량합계
                avg_maxt=('maxt', 'mean'),  # 최고기온평균
                avg_mint=('mint', 'mean'),  # 최저기온평균
                avg_tavg=('tavg', 'mean'),  # 평균기온평균
                avg_humid=('humid', 'mean'),  # 평균습도
                total_sunhours=('sunhours', 'sum'),  # 일조시간합계
                avg_wind=('wind', 'mean')  # 평균풍속
            ).reset_index()

            # 4) 파일명 및 경로 결정
            monthly_filename = os.path.join(cache_dir, f"{stn_id}_월별.csv")
            weekly_filename = os.path.join(cache_dir, f"{stn_id}_주차별.csv")

            # 5) CSV로 저장 (한글 깨짐 방지용 utf-8-sig)
            monthly.to_csv(monthly_filename, index=False, encoding='utf-8-sig')
            weekly.to_csv(weekly_filename, index=False, encoding='utf-8-sig')

            print(f"[저장] {station_name}({stn_id}) 월별 → {monthly_filename}")
            print(f"[저장] {station_name}({stn_id}) 주차별 → {weekly_filename}")
        else:
            print(f"[경고] station {stn_id}({station_name})에 유효한 데이터가 없습니다.")


def interpolate_weather(df):
    df = df.interpolate(method='linear')
    df = df.fillna(method='ffill')  # 전방 채우기
    df = df.fillna(method='bfill')  # 후방 채우기
    return df

def main():
    start = 1984
    end = 2024
    output_path = './download_weather'

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    file_path = './input/지점코드.csv'
    stations = pd.read_csv(file_path)

    dw_weather_multiple(stations, start, end, output_path)


if __name__ == '__main__':
    main()