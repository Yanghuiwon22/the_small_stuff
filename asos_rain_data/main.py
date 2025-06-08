import matplotlib.pyplot as plt
import pandas as pd
import os

import koreanize_matplotlib


def draw_fig(file_path):
    file_path = r'output/90/90_속초_monthly.csv'

    df = pd.read_csv(file_path, encoding='utf-8-sig')
    print(df.head())

    # 히스토그램으로 그리기
    plt.figure(figsize=(10, 6))
    plt.hist(df['rainy_day'], bins=12, edgecolor='black', alpha=0.7)

    # 라벨과 제목 추가
    plt.title('속초 월별 우천일수 분포')
    plt.xlabel('우천일수')
    plt.ylabel('빈도')
    plt.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('output/90/90_속초_monthly_histogram.png', dpi=300)
    plt.show()

def main():
    input_dir = './output'
    output_path = './graph_output'

    start = 1984
    end = 2024

    file_path = './input/지점코드.csv'
    stations = pd.read_csv(file_path)

    cal_monthly_data(stations, start, end, input_dir, output_path)
    cal_weekly_data(stations, start, end, input_dir, output_path)

if __name__ == '__main__':
    main()