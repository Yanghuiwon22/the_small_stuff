import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import BoundaryNorm, LinearSegmentedColormap
from matplotlib.patches import Patch
import pandas as pd
import koreanize_matplotlib
import matplotlib.image as mpimg
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
# 파일 경로
shp_path = "data/korea_shp/TL_SCCO_SIG_fixed.shp"
excel_path = r"data/korea.xlsx"
output_path = "output/korea_shp.png"

# 1. 데이터 불러오기 및 병합
df = pd.read_excel(excel_path)
df['sigun_nm'] = df['sigun_nm'].astype(str).str.strip()
gdf = gpd.read_file(shp_path)
gdf['SIG_KOR_NM'] = gdf['SIG_KOR_NM'].astype(str).str.strip()
merged_geo = gdf.merge(df[['sigun_nm', 'index_2005']], left_on='SIG_KOR_NM', right_on='sigun_nm', how='left')

# 2. 색상 및 계급 설정 (5단계)
bounds = [0, 0.2, 0.5, 1.0, 1.5, merged_geo['index_2005'].max() + 0.1]
colors = ['#2f5d38', '#659d69', '#a3ce9e', '#d1e7d1', '#edf7ec']
cmap = LinearSegmentedColormap.from_list("custom_colormap", colors, N=len(bounds)-1)
norm = BoundaryNorm(bounds, ncolors=len(colors), clip=True)

# 3. 시각화
fig, ax = plt.subplots(figsize=(24, 16))
merged_geo.plot(
    column='index_2005',
    cmap=cmap,
    linewidth=0.3,
    edgecolor='black',
    norm=norm,
    ax=ax
)

# 4. 범례
legend_labels = [
    'High Extinction Risk',
    'Emerging Extinction Risk',
    'Warning Stage',
    'Moderate Extinction Risk',
    'Low Extinction Risk'
]

legend_elements = [
    Patch(facecolor=colors[i], label=legend_labels[i])
    for i in range(len(legend_labels))
]
ax.legend(handles=legend_elements, bbox_to_anchor=(1.2, 0.05), loc='lower right', frameon=False, fontsize=30)

compass_img = mpimg.imread('data/compass.png')
imagebox = OffsetImage(compass_img, zoom=0.4)  # zoom으로 크기 조정

# (x, y) 위치 지정 - axes fraction 기준 (예: 오른쪽 아래)
ab = AnnotationBbox(imagebox, (-0.24, 0.89),  # x, y in axes fraction
                    xycoords='axes fraction',
                    frameon=False)
ax.add_artist(ab)
fig.subplots_adjust(left=0.07, right=1, top=0.95, bottom=0.05)


# 5. 제목 및 마무리
# ax.set_title("대한민국 시군구별 인구 감소", fontsize=20, fontweight='bold')
ax.axis('off')
# plt.tight_layout()
fig.savefig(output_path, dpi=300)
plt.show()