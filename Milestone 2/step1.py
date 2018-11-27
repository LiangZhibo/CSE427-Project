import sys;
import re;
from pyspark import SparkContext;
import matplotlib
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
##import pandas as pd
import io

if __name__ == "__main__":
    sc = SparkContext();
    data_raw = sc.textFile("file:/home/cloudera/training_materials/dev1/data/devicestatus.txt") \
        .map(lambda line : line.replace("|", ",")) \
        .map(lambda line : line.replace("/", ","));
    data_filter = data_raw.map(lambda line: line.split(',')) \
        .filter(lambda res : len(res) == 14 ) \
        .map(lambda tokens : (tokens[12], tokens[13], tokens[0], u'manufacture ' + tokens[1].split(" ")[0], u'model ' + tokens[1].split(" ")[1] , tokens[2])) \
        .filter(lambda tokens : tokens[0] != u'0' or tokens[2] != u'0' ) \
        .map(lambda res : ','.join(str(d) for d in res));
    # data_filter.saveAsTextFile("/loudacre/devicestatus_etl");

    latitude = data_filter.map(lambda line: line.split(',')).map(lambda line: line[0]).collect()
    latitude = [float(i) for i in latitude];
    # Get the longitude and convert to python list
    longitude = data_filter.map(lambda line: line.split(',')).map(lambda line: line[1]).collect()
    longitude = [float(i) for i in longitude];
    # determine range to print based on min, max lat and lon of the data
    margin = 10
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
                area_thresh=0.0000001,
                )
    print(lat_min)
    print(lat_max)
    print(lon_min)
    print(lon_max)
    # m.drawcoastlines()
    # m.drawcountries()
    # m.drawstates()
    # m.drawmapboundary(fill_color='#46bcec')
    m.fillcontinents(color = 'grey',lake_color='grey')
    # convert lat and lon to map projection coordinates
    lons, lats = m(longitude, latitude)
    # plot points as red dots
    m.scatter(lons, lats, s=0.0000001, marker = 'o', color='r', zorder=5)
    plt.show()


    

