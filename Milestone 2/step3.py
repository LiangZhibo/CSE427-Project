import sys;
import re;
from pyspark import SparkContext;
import matplotlib
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
##import pandas as pd
import io

if __name__ == "__main__":
    sc = SparkContext()
    data_filter = sc.textFile('file:/home/cloudera/Desktop/lat_longs') \
    .filter(lambda line : len(line) > 0) \
    .map(lambda line : line.split(' ')) \
    .map(lambda tokens : (tokens[0], tokens[1])) \
    .map(lambda res : ','.join(str(d) for d in res))

    latitude = data_filter.map(lambda line: line.split(',')).map(lambda line: line[0]).collect()
    latitude = [float(i) for i in latitude];
    longitude = data_filter.map(lambda line: line.split(',')).map(lambda line: line[1]).collect()
    longitude = [float(i) for i in longitude];
    margin = 2
    lat_min = min(latitude) - margin
    lat_max = max(latitude) + margin
    lon_min = min(longitude) - margin
    lon_max = max(longitude) + margin
	
    # create map using BASEMAP
    m = Basemap(llcrnrlon=lon_min,
                llcrnrlat=lat_min,
                urcrnrlon=lon_max,
                urcrnrlat=lat_max,
                lat_0=(lat_max - lat_min)/2,
                lon_0=(lon_max-lon_min)/2 + 90,
                projection='cyl',
                resolution = 'f',
                area_thresh=0.001,
                )


    m.fillcontinents(color = 'grey',lake_color='grey')
    # convert lat and lon to map projection coordinates
    lons, lats = m(longitude, latitude)
    # plot points as red dots
    m.scatter(lons, lats, s=0.001, marker = 'o', color='r', zorder=5)
    plt.show()

