import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

def unzip_func():
    # 압축 해제할 zip 파일 목록
    list_dir = os.listdir('US/BEFORE')
    print(list_dir)

    import zipfile

    for file_name in list_dir:
        file_name = file_name.replace('.zip', '')
        print(file_name)
        with zipfile.ZipFile(f'BEFORE/{file_name}.zip', 'r') as zip_ref:
            zip_ref.extractall(f'AFTER')  # 폴더가 없으면 자동 생성됨

def get_shp(shp_dir, output_dir):
    shp_files = [os.path.join(shp_dir, f) for f in os.listdir(shp_dir) if f.endswith(".shp")]
    gdfs = [gpd.read_file(shp) for shp in shp_files]
    # 병합
    merged_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))

    # 저장
    merged_gdf.to_file(output_dir)

def check_data():
    df = pd.read_csv('data/LMI.csv')
    df = df[(df['STATE'] == 8) & (df['LOWMOD_PCT'] > 50)]
    print(df)

def check_state_data():
    df = pd.read_csv("data/LMI.csv")

    # Load the USA map shapefile
    state_track = gpd.read_file("US/AFTER/cb_2018_08_tract_500k.shp")
    state_track['GEOID'] = state_track['GEOID'].astype(str)
    # oklahoma_tracts.to_csv('data/oklahoma_tracts.csv', index=False)

    # us_shp_file = gpd.read_file("cb_2018_us_division_500k/cb_2018_us_division_500k.shp")

    # Filter the data for Oklahoma
    df_ok_LMI = df[df['STATE'] == 8]
    df_ok_LMI = df_ok_LMI[['GEOID', 'STATE', 'COUNTY', 'LOWMOD_PCT']]
    df_ok_LMI['GEOID'] = df_ok_LMI['GEOID'].astype(str).str.strip()
    df_ok_LMI['GEOID'] = df_ok_LMI['GEOID'].apply(lambda x: str(x).zfill(11))
    #
    merged_data = state_track.merge(df_ok_LMI, left_on='GEOID', right_on='GEOID', how='left')
    print(df_ok_LMI['GEOID'], merged_data['GEOID'])
    # merged_data.dropna(inplace=True)
    print(merged_data.head())

    # Plotting
    fig, ax = plt.subplots(figsize=(24, 16))

    state_track.plot(ax=ax, color='b', edgecolor='lightgray')
    merged_gdf = gpd.GeoDataFrame(merged_data, geometry='geometry')
    merged_gdf.plot(
        column='LOWMOD_PCT',
        ax=ax,
        legend=True,
        cmap='OrRd',  # 적절한 컬러맵 선택
    )

    plt.axis('off')  # 축 제거
    plt.tight_layout()
    plt.show()

def main():
    os.makedirs('output', exist_ok=True)

    # os.makedirs("US/AFTER", exist_ok=True)
    # unzip_func()
    get_shp("de_shp", "german/german.shp")
    # check_data()
    # check_state_data()

if __name__ == "__main__":
    main()