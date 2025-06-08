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
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}

    current_year = datetime.today().year
    current_date = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

    for index, station in stations.iterrows():
        stn_id = station['지점코드']
        latitude = station['위도']
        longitude = station['경도']
        altitude = station['고도']
        station_name = station['지점명']

        for y in tqdm.tqdm(range(start, end + 1), desc=f"Downloading {station_name} ({stn_id})"):
            if y != 2025:
                end_date = current_date if y == current_year else f"{y}1231"
            else:
                end_date = f"{y}0131"

            # 연도별 폴더 경로 생성
            cache_dir = os.path.join(output_path, "cache_weather", str(stn_id), str(y))
            os.makedirs(cache_dir, exist_ok=True)

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

            weather['year'] = pd.to_datetime(weather['tm']).dt.year
            weather['month'] = pd.to_datetime(weather['tm']).dt.month
            weather['day'] = pd.to_datetime(weather['tm']).dt.day

            weather['date'] = pd.to_datetime(weather[['year', 'month', 'day']])
            weather['doy'] = weather['date'].dt.strftime('%j')

            # Selecting and renaming columns
            li = ['year', 'month', 'day', 'doy', 'sumGsr', 'maxTa', 'minTa', 'sumRn', 'sumSmlEv',
                  'avgTa', 'avgRhm', 'avgWs', 'maxWd', 'sumSsHr']
            weather = weather.loc[:, li]
            weather = weather.apply(pd.to_numeric, errors='coerce')
            list_dfs = [weather]

            # Concatenate and save
            if list_dfs:
                df = pd.concat(list_dfs)
                df.columns = ['year', 'month', 'day', 'doy', 'radn', 'maxt', 'mint', 'rain', 'evap',
                              'tavg', 'humid', 'wind', 'winddir', 'sunhours']

                df = df[['year', 'month', 'day', 'doy', 'radn', 'maxt', 'mint', 'rain', 'tavg', 'humid', 'wind', 'winddir','sunhours']]

                df['rain'] = df['rain'].fillna(0)
                df['latitude'] = latitude
                df['longitude'] = longitude
                df['altitude'] = altitude
                df = pm_weather(df, latitude, altitude)
                df = interpolate_weather(df)

                df.to_csv(cache_filename, index=False)
                print(f"Saved data for station {station_name} ({stn_id}) to {cache_filename}.")

def interpolate_weather(df):
    df.interpolate(method='linear', inplace=True)
    df.fillna(method='ffill', inplace=True)  # 전방 채우기
    df.fillna(method='bfill', inplace=True)  # 후방 채우기
    return df

def pm_weather(df, latitude, altitude): ### penman-monteith eqn
    lati = latitude
    alti = altitude
    height = 10

    u_2 = df['wind'] * 4.87 / np.log(67.8 * height - 5.42)
    P = 101.3 * ((293 - 0.0065 * alti) / 293) ** 5.26
    delta = df['tavg'].apply(lambda x: 4098 * (0.6108 * np.exp((17.27 * x) / (x + 237.3))) / (x + 237.3) ** 2)
    gamma = 0.665 * 10 ** (-3) * P
    u_2_cal = 1 + 0.34 * u_2  # P
    Q = delta / (delta + gamma * u_2_cal)  # Q
    R = gamma / (delta + gamma * u_2_cal)  # R
    S = 900 / (df['tavg'] + 273) * u_2  # S
    e_s = df['tavg'].apply(lambda x: 0.6108 * np.exp((17.27 * x) / (x + 237.3)))
    e_a = df['humid'] / 100 * e_s
    e = e_s - e_a  # e_s-e_a
    doi = df['day']  # day of year
    dr = doi.apply(lambda x: 1 + 0.033 * np.cos(2 * 3.141592 / 365 * x))
    small_delta = doi.apply(lambda x: 0.409 * np.sin(2 * 3.141592 / 365 * x - 1.39))
    theta = lati * math.pi / 180
    w_s = np.arccos(-np.tan(theta) * small_delta.apply(lambda x: np.tan(x)))

    Ra = 24 * 60 / math.pi * 0.082 * dr * \
         (w_s * small_delta.apply(lambda x: math.sin(x)) *
          np.sin(theta) +
          np.cos(theta) *
          small_delta.apply(lambda x: math.cos(x)) *
          w_s.apply(lambda x: math.sin(x)))
    N = 24 / math.pi * w_s
    Rs = (0.25 + 0.5 * df['sunhours'] / N) * Ra
    Rso = (0.75 + 2 * 10 ** (-5) * alti) * Ra
    Rs_Rso = Rs / Rso  # Rs/Rso
    R_ns = 0.77 * Rs
    R_nl = 4.903 * 10 ** (-9) * (df['tavg'] + 273.16) ** 4 * (0.34 - 0.14 * e_a ** (0.5)) * (
            1.35 * Rs_Rso - 0.35)
    G = 0
    ET = ((0.408) * (delta) * (R_ns - R_nl - G) + (gamma) * (900 / (df['tavg'] + 273)) * u_2 * (e)) / (
            delta + gamma * (1 + 0.34 * u_2))

    df['radn'] = df['radn'].fillna(round(R_ns, 3))
    return df


def main():
    start = 1984
    end = 2025
    output_path = './download_weather'

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    file_path = './input/지점코드.csv'
    stations = pd.read_csv(file_path)

    dw_weather_multiple(stations, start, end, output_path)


if __name__ == '__main__':
    main()