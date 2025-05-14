import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
from matplotlib.colors import LinearSegmentedColormap, ListedColormap
import numpy as np


def xlsx_to_csv():
    data_list = os.listdir("../data")

    for file in data_list:
        if file.endswith(".xlsx"):
            xlsx_file = os.path.join("../data", file)
            csv_file = os.path.join("../data", file.replace(".xlsx", ".csv"))

            # Read the Excel file
            df = pd.read_excel(xlsx_file, sheet_name=None)

            # Save each sheet as a separate CSV file
            for sheet_name, sheet_df in df.items():
                sheet_df.to_csv(f"{csv_file}", index=False)
                print(f"Converted {csv_file} to CSV.")
                os.remove(xlsx_file)

def data_preprocessing():
    df = pd.read_csv("../data/ACS-2020-Low-Mod-Block-Group-All.csv")
    df = df[['GEOID', 'GEONAME', 'STUSAB', 'COUNTYNAME', 'STATE', 'COUNTY', 'TRACT',
       'BLKGRP', 'LOWMOD', 'LOWMODUNIV', 'LOWMOD_PCT',
       'MOE_LOWMODPCT']]

    df['LOWMOD_PCT'] = df['LOWMOD_PCT'].astype(str).str.replace('%', '').astype(float)
    df['GEOID'] = df['GEOID'].apply(lambda x: str(x)[:-1])
    df['LOWMOD'] = df['LOWMOD'].astype(str).str.replace(",", "").astype(float)
    df['LOWMODUNIV'] = df['LOWMODUNIV'].astype(str).str.replace(",", "").astype(float)

    grouped = df.drop(columns=['GEONAME', 'STUSAB', 'COUNTYNAME', 'BLKGRP', 'MOE_LOWMODPCT'], axis=1).groupby(['GEOID', 'STATE', 'COUNTY', 'TRACT'])
    df_LMI = pd.DataFrame()
    for columns, group in tqdm(grouped):
        # Create a new DataFrame for each group
        new_dict = {
            'GEOID': columns[0],
            'STATE': columns[1],
            'COUNTY': columns[2],
            'TRACT': columns[3],
            'LOWMOD_PCT': round(group['LOWMOD'].sum() / group['LOWMODUNIV'].sum() * 100, 2)
        }
        df_new = pd.DataFrame([new_dict])
        df_LMI = pd.concat([df_LMI, df_new], ignore_index=True)

    df_LMI.to_csv('data/LMI.csv', index=False)


def load_usa_map():
    df = pd.read_csv("../data/LMI.csv")

    # Load the USA map shapefile
    oklahoma_tracts = gpd.read_file("../data/merged_us_shp/merged_us_states.shp")
    oklahoma_tracts['GEOID'] = oklahoma_tracts['GEOID'].astype(str)
    # oklahoma_tracts.to_csv('data/oklahoma_tracts.csv', index=False)

    us_shp_file = gpd.read_file("cb_2018_us_division_500k/cb_2018_us_division_500k.shp")

    # Filter the data for Oklahoma
    df_ok_LMI = df[['GEOID', 'STATE', 'COUNTY', 'LOWMOD_PCT']]
    df_ok_LMI['GEOID'] = df_ok_LMI['GEOID'].astype(str).str.strip()
    df_ok_LMI['GEOID'] = df_ok_LMI['GEOID'].apply(lambda x: str(x).zfill(11))

    merged_data = oklahoma_tracts.merge(df_ok_LMI, left_on='GEOID', right_on='GEOID', how='left')
    # merged_data = merged_data[merged_data['LOWMOD_PCT'] > 50]
    #
    # # Plotting
    fig, ax = plt.subplots(figsize=(24, 16))
    #
    oklahoma_tracts.plot(ax=ax, color='none', edgecolor='lightgray')

    cmap_reds = plt.cm.Reds
    reds_part = cmap_reds(np.linspace(0.3, 1.0, 128))  # 빨간색 계열 128단계

    # 흰색 + 빨강 계열 결합
    white_part = np.ones((128, 4))  # 128단계 흰색 (RGBA: 1,1,1,1)
    white_part[:, 3] = 0  # Alpha(불투명도) → 0 = 완전 투명
    custom_colors = np.vstack([white_part, reds_part])  # 총 256단계1.0, 256)))

    custom_cmap = ListedColormap(custom_colors)

    merged_gdf = gpd.GeoDataFrame(merged_data, geometry='geometry')
    merged_gdf.plot(
        column='LOWMOD_PCT',
        ax=ax,
        legend=True,
        cmap=custom_cmap,  # 적절한 컬러맵 선택
    )

    ticks = np.linspace(0, 100, 11)
    cbar_ax = ax.get_figure().axes[-1]

    cbar_ax.set_ylabel("LMI index", fontsize=30)  # label 폰트 크기 조절
    cbar_ax.set_yticks(ticks)
    cbar_ax.tick_params(labelsize=30)

    plt.axis('off')  # 축 제거
    plt.tight_layout()
    # plt.show()
    plt.savefig('output/미국_최종본_03.png', dpi=300, bbox_inches='tight')
def main():
    # 1. xlsx to csv
    os.makedirs('output', exist_ok=True)
    xlsx_to_csv()

    # 2. data preprocessing
    if not os.path.exists('../data/LMI.csv'):
        data_preprocessing()

    # 3. load USA geo data
    load_usa_map()


if __name__ == "__main__":
    main()