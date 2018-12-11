import io
import re
import sys

from pyspark import SparkContext
from HelpFunction import *

# K-means algorithm
if __name__ == '__main__':
	sc = SparkContext()

	# Manage the input argument
	if len(sys.argv) != 5:
		print('Invalid argument format')
		sys.exit(-1)

	input_path = sys.argv[1]
	output_path = sys.argv[2]
	method = sys.argv[3]
	k = sys.argv[4]

	if method != 'EuclideanDistance' and method != 'GreatCircleDistance':
		print('Invalid method input. The optionals are EuclideanDistance and GreatCircleDistance')
		sys.exit(-1)

	if k <= 0:
		print('Invalid k value, k should be positive number')
		sys.exit(-1)

	# Parse the input file into (latitude, longtitude) pairs and persist the resulting RDD in cache
	data = sc.textFile(input_path).map(lambda x: x.split(',')).map(lambda x: LonLatPoint(x[0], x[1])).cache()

	# Implement the k-means algorithm

	# Initially choose k points to be the centroid of k clusters
	center = data.takeSample(False, int(k), 1)
	for i in center:
			print i;
	index = -1;
	# Initial the total location change of the center of cluster
	changeDist = float('inf')

	# Define a converge distance to decide whether the iteration of k-means is finished
	convergeDist = float(0.1)
	cluster = None
	# Start k-means iteration 
	while changeDist > convergeDist: 
		cluster = data.map(lambda x: (closestPoint(x, center, method), x))
		num = cluster.countByKey()
		# Calculate the new means of the clusters 
		newCenter = cluster.map(lambda (closest, x): (closest,LonLat_to_Cartesian(x))) \
		.reduceByKey(addPoints) \
		.map(lambda (center, sum): (center, dividePoints(sum, num[center]))) \
		.sortByKey()
		newCenter2 = newCenter.map(lambda (center, new_center): new_center)
		new_center = newCenter2.collect();
		for i in new_center:
			print i.latitude;
			print i.longtitude;
		print "dist:" + str(changeDist);
		# Calculate the distance change between two center
		distances = newCenter.map(lambda (idx, mean): EuclideanDistance(center[idx], mean));
		print "ddd"
		for d in distances.collect():
			print d;
		changeDist = distances.sum();
		print "dist3"
		center = newCenter2.collect();
	res = cluster.map(lambda (closest, point): str(closest) + "," + str(point.latitude) + "," + str(point.longtitude))
	res.saveAsTextFile(output_path)






