import os
import pandas as pd
import tqdm


def data_statics(period, group_df):
    #################################################################################
    # 강우일수, 강우량 합계, 강우량 10mm 이상, 30mm 이상, 50mm 이상, 70mm 이상, 90mm 이상, 110mm 이상
    # 평균기온, 평균최고기온, 평균최저기온, 편균 기온 10-30도 사이의 합
    # 평균습도
    # 평균풍속
    #################################################################################


    rainy_day = group_df[group_df['rain'] > 0]['rain'].count()
    total_rain = group_df['rain'].sum()
    

    weekly_data = {
        'year': group_df['year'].values[0],
        'doy': group_df['doy'].values[0],
        rf'{period}': group_df[period].values[0],
        'rainy_day': rainy_day, # 강수일수
        'total_rain': total_rain, # 강우량합계
        'tavg': group_df['tavg'].mean(), # 평균기온
        'tmin_avg': group_df['mint'].mean(), # 평균최저기온
        'tmax_avg': group_df['maxt'].mean(), # 평균최고기온,
        'tavg_10_30_between_sum': group_df[(group_df['tavg'] >= 10) & (group_df['tavg'] <= 30)]['tavg'].sum(), # 10도 이상 30도 이하의 일 수
        'humid_avg': group_df['humid'].mean(), # 평균습도
        'wind_avg': group_df['wind'].mean(), # 평균풍속
        'sunhours_sum': group_df['sunhours'].sum(), # 일조시간합계
        'radn_sum': group_df['radn'].sum(), # 일사량합계
    }
    for x in range(1, 12, 2):
        weekly_data[f'rain_more_{x * 10}'] = group_df[group_df['rain'] > x * 10]['rain'].count() # 강우량 x mm 이상

    for x in range(5, 11, 5):
        weekly_data[f'tavg_less_{x}_count'] = group_df[group_df['tavg'] <= x ]['tavg'].count() # 온도 x 이하의 일 수
        weekly_data[f'tavg_less_{x}_sum'] = group_df[group_df['tavg'] <= x ]['tavg'].sum() # 온도 x 이하의 일 수
        
    for x in range(15, 41, 5):
        weekly_data[f'tavg_more_{x}_count'] = group_df[group_df['tavg'] >= x ]['tavg'].count() # 온도 x 이하의 일 수
        weekly_data[f'tavg_more_{x}_sum'] = group_df[group_df['tavg'] >= x ]['tavg'].sum() # 온도 x 이하의 일 수



    
    return weekly_data

def cal_monthly_data(stations, start, end, input_path, output_path):
    for index, station in stations.iterrows():
        station_code = station['지점코드']
        station_name = station['지점명']

        df_monthly = pd.DataFrame()
        for y in tqdm.tqdm(range(start, end + 1), desc=f"processing {station_name} ({station_code})"):
            cache_dir = os.path.join(input_path, "cache_weather", str(station_code), str(y))
            cache_filename = os.path.join(cache_dir, f"{station_code}_{station_name}_{y}.csv")

            try:
                df = pd.read_csv(cache_filename, encoding='utf-8-sig')
                grouped_df = df.groupby(['year', 'month', 'latitude', 'longitude', 'altitude'])
                for idx, group in grouped_df:
                    rainy_day = group[group['rain'] > 0]['rain'].count()
                    total_rain = group['rain'].sum()

                    monthly_data = data_statics('month', group)

                    df_monthly = pd.concat([df_monthly, pd.DataFrame([monthly_data])], ignore_index=True)
            except FileNotFoundError:
                print(f"File not found: {cache_filename}")
                continue

        output_folder_path = os.path.join(output_path, f'{station_code}')
        os.makedirs(output_folder_path, exist_ok=True)

        output_file_path = os.path.join(output_folder_path, f"{station_code}_{station_name}_monthly.csv")
        df_monthly.to_csv(output_file_path)

def cal_weekly_data(stations, start, end, input_path, output_path):
    for index, station in stations.iterrows():
        station_code = station['지점코드']
        station_name = station['지점명']

        df_weekly = pd.DataFrame()
        df_all = pd.DataFrame()
        for y in tqdm.tqdm(range(start, end + 2), desc=f"processing {station_name} ({station_code})"):
            # print(y)
            cache_dir = os.path.join(input_path, "cache_weather", str(station_code), str(y))
            cache_filename = os.path.join(cache_dir, f"{station_code}_{station_name}_{y}.csv")

            try:
                df = pd.read_csv(cache_filename, encoding='utf-8-sig')
                df_all = pd.concat([df_all, df], ignore_index=True)
            except:
                print(f"File not found: {cache_filename}")
                continue

        df_all['date'] = df_all['year'].astype(str) + '-' + df_all['month'].astype(str).str.zfill(2) + '-' + df_all[
            'day'].astype(
            str).str.zfill(2)

        df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce')
        df_all['iso_year'] = df_all['date'].dt.isocalendar().year
        df_all['week'] = df_all['date'].dt.isocalendar().week
        df_all = df_all[df_all['iso_year'].between(start, end)]
        df_all.to_csv('df_all.csv', index=False)
        grouped_df = df_all.groupby(['iso_year', 'week', 'latitude', 'longitude', 'altitude'])


        for idx, group in grouped_df:
            week_num = idx[1]

            print(f"=================={idx[0]}===================")
            print(group)
            print(idx[0], week_num, group['week'].count())

            if group['week'].count() != 7 and idx[0] != '2025':
                print(f"Skipping week {week_num} of year {idx[0]} due to insufficient data.")

            weekly_data = data_statics('week', group)

            df_weekly = pd.concat([df_weekly, pd.DataFrame([weekly_data])], ignore_index=True)

        output_folder_path = os.path.join(output_path, f'{station_code}')
        os.makedirs(output_folder_path, exist_ok=True)

        output_file_path = os.path.join(output_folder_path, f"{station_code}_{station_name}_weekly.csv")
        df_weekly.to_csv(output_file_path)

def main():
    input_dir = 'download_weather'
    output_path = './output'

    start = 1984
    end = 2024

    file_path = './input/지점코드.csv'
    stations = pd.read_csv(file_path)

    cal_monthly_data(stations, start, end, input_dir, output_path)
    cal_weekly_data(stations, start, end, input_dir, output_path)


if __name__ == '__main__':
    main()
