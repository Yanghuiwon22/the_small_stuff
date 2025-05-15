import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import matplotlib.patches as mpatches
import os
import matplotlib.image as mpimg
from matplotlib.offsetbox import OffsetImage, AnnotationBbox

plt.rcParams['font.family'] ='Malgun Gothic'
plt.rcParams['axes.unicode_minus'] =False

def load_map():
    #     # df = pd.read_csv("../data/LMI.csv")
    #
    # Load the Germany map shapefile
    base_shp = gpd.read_file("data/germany_shp/georef-germany-kreis-millesime.shp")
    print(base_shp.crs)

    base_shp_table = base_shp.copy()
    base_shp_table['lan_name'] = base_shp_table['lan_name'].apply(lambda x: str(x)[2:-2])
    base_shp_table['krs_name_sh'] = base_shp_table['krs_name_sh'].apply(lambda x: str(x)[2:-2])

    yellow_region = ['Greiz', 'Ludwigslust-Parchim', 'Rostock', 'Vorpommern-Rügen', 'Mecklenburgische Seenplatte', 'Prignitz', 'Zwickau',
                     'Ludwigslust-Parchim', 'Havelland', 'Stendal', 'Ostprignitz-Ruppin','Nordwestmecklenburg','Schwerin', 'Brandenburg an der Havel', 'Jerichower Land', 'Börde', 'Harz'
                     , 'Landkreis Salzlandkreis', 'Wittenberg','Dithmarschen', 'Cuxhaven', 'Dessau-Roßlau', 'Anhalt-Bitterfeld',
                     'Oberspreewald-Lausitz', 'Elbe-Elster', 'Nordhausen', 'Kyffhäuserkreis', 'Gotha', 'Mansfeld-Südharz',
                     'Landkreis Salzlandkreis', 'Landkreis Burgenlandkreis', 'Landkreis Saalekreis', 'Landkreis Kyffhäuserkreis', 'Landkreis Unstrut-Hainich-Kreis',
                     'Landkreis Wartburgkreis', 'Schmalkalden-Meiningen', 'Lüchow-Dannenberg', 'Hildburghausen','Sonneberg', 'Saalfeld-Rudolstadt', 'Landkreis Saale-Orla-Kreis',
                     'Gera', 'Altenburger Land', 'Suhl', 'Landkreis Altmarkkreis Salzwedel', 'Friesland', 'Wilhelmshaven',
                     'Birkenfeld', 'Südwestpfalz', 'Remscheid', 'Hamm', 'Hagen', 'Herne', 'Bottrop', 'Oberhausen', 'Pirmasens',
                     'Zweibrücken', 'Worms', 'Landkreis Werra-Meißner-Kreis']
    orange_region = ['Vorpommern-Greifswald', 'Uckermark', 'MärkischOderland', 'Märkisch-Oderland', 'Frankfurt (Oder)', 'Oder-Spree', 'Spree-Neiße',
                    'Cottbus', 'Görlitz', 'Bautzen', 'Mittel-sachsen', 'Erzgebirgs-kreis','Vogtland-kreis', 'Uckermark', 'Landkreis Vogtlandkreis', 'Landkreis Erzgebirgskreis','Mittelsachsen']
    stripe_CD = ['Wesel', 'Hof', 'Wunsiedel i.Fichtelgebirge','Sächsische Schweiz-Osterzgebirge','Tirschenreuth', 'Sömmerda',
                 'Nordsachsen', 'Chemnitz', 'Leipzig', 'Duisburg', 'Recklinghausen', 'Dortmund', 'Marl', 'Gelsenkirchen', 'Unna',
                 'Saar-louis', 'Landkreis Regionalverband Saarbrücken', 'Wuppertal', 'Bochum', 'Mülheim an der Ruhr', 'Essen', 'Mönchengladbach']
    green_region = ['Neumünster','Meißen', 'Barnim', 'Oberhavel', 'PotsdamMittelmark', 'Saarlouis',
                    'Teltow-Fläming', 'Dahme-Spreewald', 'Lübeck','Regen', 'Potsdam-Mittelmark',
                    'Weiden i.d.OPf.', 'Neustadt a.d.Waldnaab','Nordfriesland', 'Dresden', 'Leipzig',
                    'Lübeck', 'Ostholstein', 'Plön', 'Kiel', 'Steinburg', 'Rendsburg-Eckernförde', 'Steinburg',
                    'Nordfriesland', 'Schleswig-Flensburg', 'Neumünster',
                    'Barnim', 'Oberhavel', 'PotsdamMittelmark', 'Rotenburg (Wümme)', 'Rotenburg (Wümme)',
                    'Nienburg (Weser)', 'Celle', 'Uelzen', 'Cuxhaven',
                    'Osterholz', 'Rotenburg (Wümme)', 'Diepholz', 'Nienburg (Weser)', 'Celle',
                    'Uelzen', 'Landkreis Heidekreis', 'Bremerhaven', 'Delmenhorst', 'Oldenburg', 'Leer', 'Emden', 'Aurich',
                    'Wittmund', 'Ammerland', 'oldenburg', 'Cloppenburg', 'Osnabrück', 'Herford', 'Lippe', 'Schaumburg',
                    'Hameln-Pyrmont', 'Holz-minden', 'Northeim', 'Holzminden', 'Northeim', 'Göttingen', 'Hannover',
                    'Goslar', 'Eichsfeld', 'Helmstedt', 'Kronach', 'Landkreis Saale-Holzland-Kreis', 'Weimarer Land',
                    'Landkreis Ilm-Kreis', 'Erfurt', 'Weimar', 'Jena', 'Kleve', 'Viersen', 'Heinsberg', 'Euskirchen', 'Ahrweiler', 'Vulkaneifel',
                    'Cochem-Zell', 'Bernkastel-Wittlich', 'Bernkastel-Wittlich', 'Bad Kreuznach', 'Trier'
                    ,'Merzig-Wadern', 'Landkreis Rhein-Hunsrück-Kreis', 'Kusel', 'Landkreis Donnersbergkreis', 'Kaiserslautern',
                    'Landkreis Odenwaldkreis', 'Landkreis Vogelsbergkreis', 'Waldeck-Frankenberg', 'Kreis Hochsauerlandkreis',
                    'Kreis Märkischer Kreis', 'Kreis Oberbergischer Kreis', 'Altenkirchen (Westerwald)', 'Solingen', 'Kreis Ennepe-Ruhr-Kreis'
                    , 'Neunkirchen', 'Höxter']
    stripe_NC = ['Schwandorf', 'Cham', 'Paderborn', 'Freyung-Grafenau', 'Düren', 'Kreis Städteregion Aachen',
                 'Landkreis Saarpfalz-Kreis', 'St. Wendel', 'Bielefeld']

    green_land_shp = base_shp_table.copy()
    green_land_shp['stage'] = '-'
    green_land_shp.loc[green_land_shp['krs_name_sh'].isin(green_region), 'stage'] = 'D'
    green_land_shp.loc[green_land_shp['krs_name_sh'].isin(stripe_CD), 'stage'] = 'C&D'
    green_land_shp.loc[green_land_shp['krs_name_sh'].isin(orange_region), 'stage'] = 'C'
    green_land_shp.loc[green_land_shp['krs_name_sh'].isin(yellow_region), 'stage'] = 'C_Y'
    green_land_shp.loc[green_land_shp['krs_name_sh'].isin(stripe_NC), 'stage'] = 'D&N'


    green_land_shp['color'] = green_land_shp['stage'].apply(
        lambda x: '#0B7a26' if x == 'D' else '#81c147' if x == 'C&D' else 'orange' if x == 'C' else '#348171' if x=='D&N' else 'yellow' if x =='C_Y' else 'None')
    print(green_land_shp['stage'].value_counts())

    # Plotting
    fig, ax = plt.subplots(figsize=(24, 16))
    compass_img = mpimg.imread('data/compass.png')
    imagebox = OffsetImage(compass_img, zoom=0.4)  # zoom으로 크기 조정

    # (x, y) 위치 지정 - axes fraction 기준 (예: 오른쪽 아래)
    ab = AnnotationBbox(imagebox, (-0.2, 0.85),  # x, y in axes fraction
                        xycoords='axes fraction',
                        frameon=False)
    ax.add_artist(ab)

    base_shp.plot(ax=ax, color='lightgray', edgecolor='gray')

    # plt.tight_layout()

    # colors = ['green' if stage == 'D' else 'lightgray' for stage in green_land_shp['stage']]
    green_land_shp.plot(ax=ax, color=green_land_shp['color'], edgecolor='darkgray', legend=True,)
    # fig.subplots_adjust(left=0.07, right=1.13, top=0.95, bottom=0.05)

    legend_patches = [
        mpatches.Patch(color='#0B7a26', label='D grade'),
        mpatches.Patch(color='#81c147', label='C+D grade'),
        mpatches.Patch(color='orange', label='C grade with \nborder bonus (para. 184)'),
        mpatches.Patch(color='yellow', label='C grade'),
        mpatches.Patch(color='#348171', label='D+Non grade'),
        mpatches.Patch(color='lightgray', label='Non area')
    ]
    # ax.legend(handles=legend_patches, loc='lower right', frameon=False, bbox_to_anchor=(0.9, 0.15))

    ax.legend(
        handles=legend_patches,
        title='Classification of GRW \nSupport Regions (2022 – 2027)',
        title_fontsize=30,
        loc=[1.2, 0.05],
        frameon=False,
        fontsize=30,
    )
    fig.subplots_adjust(left=0.07, right=0.7, top=0.95, bottom=0.05)

    plt.axis('off')  # 축 제거
    # plt.show()
    plt.savefig('output/독일_최종본2.png', dpi=300)
#
def main():
    os.makedirs('output', exist_ok=True)

    load_map()


if __name__ == "__main__":
    main()
