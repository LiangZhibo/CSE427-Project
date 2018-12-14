import io
import re
import sys
import time

from pyspark import SparkContext
from HelpFunction import *

# K-means algorithm
if __name__ == '__main__':
	# start_time = time.time()
	sc = SparkContext()

	# Manage the input argument
	if len(sys.argv) != 6:
		print('Invalid argument format')
		sys.exit(-1)

	input_path = sys.argv[1]
	output_path = sys.argv[2]
	method = sys.argv[3]
	k = sys.argv[4]
	center_path = sys.argv[5]

	if method != 'EuclideanDistance' and method != 'GreatCircleDistance':
		print('Invalid method input. The optionals are EuclideanDistance and GreatCircleDistance')
		sys.exit(-1)

	if k <= 0:
		print('Invalid k value, k should be positive number')
		sys.exit(-1)

	# Parse the input file into (latitude, longtitude) pairs and persist the resulting RDD in cache
	data = sc.textFile('file://' + input_path).map(lambda x: x.split(',')).map(lambda x: LonLatPoint(x[0], x[1])).cache()

	# Implement the k-means algorithm
	print('Start K-means algorithm')

	# Initially choose k points to be the centroid of k clusters
	center = data.takeSample(False, int(k), 1)

	index = -1
	# Initial the total location change of the center of cluster
	changeDist = float('inf')

	# Define a converge distance to decide whether the iteration of k-means is finished
	convergeDist = float(0.1)
	cluster = None
	# Start k-means iteration 
	while changeDist > convergeDist: 
		# Update the membership of each data point
		cluster = data.map(lambda x: (closestPoint(x, center, method), x))
		# Calculate the number of each cluster
		num = cluster.countByKey()
		# Calculate the new center of the clusters 
		newCenter = cluster.map(lambda (closest, x): (closest,LonLat_to_Cartesian(x))) \
		.reduceByKey(addPoints) \
		.map(lambda (center, sum): (center, dividePoints(sum, num[center]))) \
		.sortByKey()
		newCenter2 = newCenter.map(lambda (center, new_center): new_center)
		new_center = newCenter2.collect()
		# Calculate the distance between new and old cluster center
		if method == "EuclideanDistance":
			distances = newCenter.map(lambda (index, mean): EuclideanDistance(center[index], mean));
		else:
			distances = newCenter.map(lambda (index, mean): GreatCircleDistance(center[index], mean));
		changeDist = distances.sum()
		center = newCenter2.collect()
	print('K-means algorithm finished')
	# Calculate the running time of K-means algorithm
	# print('Running time is: ')
	# print(round(time.time() - start_time, 5))
	# Save the result to text files
	res = cluster.map(lambda (closest, point): str(closest) + "," + str(point.latitude) + "," + str(point.longtitude))
	res.coalesce(1).saveAsTextFile('file://' + output_path)
	res2 = newCenter.map(lambda (center, new_center) : str(center) + "," + str(new_center.latitude) + "," + str(new_center.longtitude))
	res2.coalesce(1).saveAsTextFile('file://' + center_path)






