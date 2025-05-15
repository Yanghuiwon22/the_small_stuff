import os

import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, BoundaryNorm, ListedColormap
from matplotlib.patches import Patch
import numpy as np
import koreanize_matplotlib
import matplotlib.image as mpimg
from matplotlib.offsetbox import OffsetImage, AnnotationBbox

def draw_map():
    # os.makedirs('data/french_shp', exist_ok=True)
    # 1. 프랑스 지도 데이터 불러오기

    output_path = "output/france_final.png"
    france = gpd.read_file('data/france_shp/france.shp')
    france.to_crs('epsg:2154', inplace=True)  # WGS 84 좌표계로 변환

    # 2. 인구 변화율 데이터 준비
    population_change_data = {
        "2": 1, "8": 1, "50": 1, "61": 1, "55": 1,
        "88": 1, "52": 1, "70": 1, "39": 1, "58": 1,
        "18": 1, "3": 1, "23": 1, "87": 1, "19": 1,
        "36": 1, "15": 1, "48": 1, "46": 1, "65": 1,
        "75": 1,
        "53": 0.7, "57": 0.7, "54": 0.7, "71": 0.7, "16": 0.7,
        "24": 0.7, "6": 0.7,
        "4": 0.3, "5": 0.3, "9": 0.3, "10": 0.3, "11": 0.3,
        "12": 0.5, "13": 0.5, "14": 0.5, "20": 0.5, "21": 0.5,
        "22": 0.5, "25": 0.5, "28": 0.5, "29": 0.5, "32": 0.5,
        "37": 0.5, "41": 0.5, "42": 0.5, "43": 0.5, "45": 0.5,
        "47": 0.5, "51": 0.5, "59": 0.5, "60": 0.5, "62": 0.5,
        "67": 0.5, "68": 0.5, "72": 0.5, "76": 0.5, "78": 0.5,
        "79": 0.5, "80": 0.5, "81": 0.5, "84": 0.5, "86": 0.5,
        "89": 0.5, "90": 0.5, "91": 0.5, "92": 0.5,
        "77": 0.2, "56": 0.2, "27": 0.2, "95": 0.2, "49": 0.2,
        "85": 0.2, "17": 0.2, "63": 0.2, "73": 0.2, "38": 0.2,
        "7": 0.2, "26": 0.2, "30": 0.2, "83": 0.2, "101": 0.2,
        "66": 0.2, "40": 0.2, "64": 0.2, "94": 0.2,
        "35": 0, "44": 0, "93": 0, "1": 0, "69": 0,
        "74": 0, "33": 0, "82": 0, "31": 0, "34": 0,
        "100": 0
    }

    # GeoDataFrame에 인구 변화율 병합
    france['taux_variation'] = france['ID_2'].astype(str).map(population_change_data)

    # 3. 사용자 정의 색상 맵 생성
    bounds = [0, 0.2, 0.4, 0.6, 0.8, 1]
    colors = ['#f2f0f7', '#cbc9e2', '#9e9ac8', '#756bb1', '#54278f'] # 색상 순서 변경: 옅은 색 -> 진한 색
    cmap = ListedColormap(colors)
    norm = BoundaryNorm(bounds, ncolors=len(colors), clip=True)

    # 4. 지도 시각화
    fig, ax = plt.subplots(figsize=(24, 16))
    france.plot(
        column='taux_variation',
        cmap=cmap,
        linewidth=0.5,
        edgecolor='gray',
        norm=norm,
        ax=ax
    )

    # 5. 범례
    legend_labels = [
        'High Extinction Risk',
        'Emerging Extinction Risk',
        'Warning Stage',
        'Moderate Extinction Risk',
        'Low Extinction Risk'
    ]

    legend_elements = [
        Patch(facecolor=colors[len(colors) - 1 - i], label=legend_labels[i]) # 색상 리스트 역순 참조
        for i in range(len(legend_labels))
    ]
    ax.legend(handles=legend_elements, bbox_to_anchor=(1.55, 0.3), frameon=False, fontsize=30)

    compass_img = mpimg.imread('data/compass.png')
    imagebox = OffsetImage(compass_img, zoom=0.4)  # zoom으로 크기 조정

    # (x, y) 위치 지정 - axes fraction 기준 (예: 오른쪽 아래)
    ab = AnnotationBbox(imagebox, (0.0, 0.96),  # x, y in axes fraction
                        xycoords='axes fraction',
                        frameon=False)
    ax.add_artist(ab)
    fig.subplots_adjust(left=0.07, right=0.65, top=0.95, bottom=0.05)

    # 6. 제목 및 마무리
    # ax.set_title("프랑스 지역별 인구 감소", fontsize=20, fontweight='bold')
    ax.axis('off')
    # plt.tight_layout()
    fig.savefig(output_path, dpi=300)
    plt.show()

def main():
    draw_map()

if __name__ == "__main__":
    main()