import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import koreanize_matplotlib
import matplotlib.image as mpimg
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
# 파일 경로
shp_path = "data/japan_shp/polbnda_jpn.shp"
excel_path = "data/02_list.xlsx"
output_path = "output/japan_v2.png"

# 1. 시정촌 경계 데이터
gdf = gpd.read_file(shp_path)
gdf = gdf.to_crs(epsg=3101)


# 2. 엑셀 인구 데이터
df = pd.read_excel(excel_path, sheet_name=0, skiprows=2)
df = df.rename(columns={
    'code': 'municipality_code',
    'ken': 'prefecture',
    'Unnamed: 2': 'municipality_name',
    '総人口': 'total_population'
})

# 3. 코드 형식 통일
df['municipality_code'] = df['municipality_code'].astype(str).str.zfill(5)
gdf['adm_code'] = gdf['adm_code'].astype(str).str.zfill(5)

# 4. 병합
merged = gdf.merge(df[['municipality_code', 'total_population']],
                   left_on='adm_code', right_on='municipality_code', how='left')

# 5. 인구 분류
merged['pop_class'] = merged['total_population'].apply(
    lambda x: 'Below 10k' if pd.notnull(x) and x < 10000
              else ('10k or more' if pd.notnull(x) and x >= 10000 else 'No Data')
)

# 6. 색상 매핑
fir_color = '#3182bd'
sec_color = '#bdd7e7'
base_color = '#eff3ff'

color_map = {
    'Below 10k': fir_color,
    '10k or more': sec_color,
    'No Data': base_color
}

# 7. 시각화 (OOP 방식)
fig = plt.figure(figsize=(24, 16))
ax = fig.add_subplot(1, 1, 1)

merged.plot(ax=ax,
            color=merged['pop_class'].map(color_map),
            linewidth=0.05,
            edgecolor='black')
merged = gdf.to_crs(epsg=3101)


# 8. 범례
legend_elements = [
    Patch(facecolor=fir_color, label='Population Under 10,000'),
    Patch(facecolor=sec_color, label='Population 10,000 and Over'),
    Patch(facecolor=base_color, label='No Data')
]
ax.legend(handles=legend_elements, loc='lower right', frameon=False, bbox_to_anchor=(1.15, 0.15), fontsize=30)

# 9. 제목 및 저장
# ax.set_title("일본 지역별 인구 감소", fontsize=20, fontweight='bold', pad=40)
compass_img = mpimg.imread('data/compass.png')
imagebox = OffsetImage(compass_img, zoom=0.4)  # zoom으로 크기 조정

# (x, y) 위치 지정 - axes fraction 기준 (예: 오른쪽 아래)
ab = AnnotationBbox(imagebox, (-0.05, 0.9),  # x, y in axes fraction
                    xycoords='axes fraction',
                    frameon=False)
ax.add_artist(ab)
fig.subplots_adjust(left=0.03, right=1, top=0.95, bottom=-0.07)


ax.set_axis_off()
# ax.set_position([0.1, 0.15, 0.8, 0.9])
# fig.tight_layout()
fig.savefig(output_path, dpi=300)
plt.show()
# plt.close(fig)