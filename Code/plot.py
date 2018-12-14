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
    data = sc.textFile("file:/home/cloudera/Desktop/final/output1/part-00000") \
    	.map(lambda line : line.split(","))
    centers = sc.textFile("file:/home/cloudera/Desktop/final/center1/part-00000") \
    	.map(lambda line: line.split(","))
    center_list = centers.collect();
    for c in centers.collect():
    	print c[0];
    	print c[1];
    	print c[2];
    	#.map(lambda token : [token[0], token[1], token[2]]])
    latitude = data.map(lambda line: line[1]).collect()
    latitude = [float(i) for i in latitude];
    longitude = data.map(lambda line: line[2]).collect()
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
                area_thresh=0.000001,
                )
    m.fillcontinents(color = 'grey',lake_color='grey')
    for index in range(0,5):
    	switcher = {
    		0: "blue",
    		1: "red",
    		2: "green",
    		3: "black",
    		4: "orange"
    	}
    	color = switcher.get(index);
    	new_data = data.filter(lambda line : line[0] == str(index).decode('utf-8')).cache()
    	latitude = new_data.map(lambda line: line[1]).collect()
    	latitude = [float(i) for i in latitude];
    	print len(latitude)
    	longitude = new_data.map(lambda line: line[2]).collect()
    	longitude = [float(i) for i in longitude];
    	print len(longitude)
    	lons, lats = m(longitude, latitude)
    	# plot centers
    	center = center_list[int(index)];
    	print center[1];
    	print center[2];
    	c_lons, c_lats = m(float(center[2]), float(center[1]))
    	m.plot(c_lons, c_lats, marker = 'D', color = color)
    	plt.text(c_lons, c_lats, str(index))
    	# plot points as red dots
    	m.scatter(lons, lats, s=0.00001, marker = 'o', color=color, zorder=5)
    plt.show()



    

