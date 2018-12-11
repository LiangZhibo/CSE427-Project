# This file define all the function needed for k-means algorithm
import math
import sys
# Earth radius constant
RADIUS = 6371.0

# Define a point on earth using longtitude and latitude
class LonLatPoint:
	def __init__(self, latitude, longtitude):
		self.longtitude = float(longtitude)
		self.latitude = float(latitude)

# Define a point on earth using Cartesian coordinate system
class CartesianPoint:
	def __init__(self, x, y, z):
		self.x = float(x)
		self.y = float(y)
		self.z = float(z)

# Convert LonLatPoint to CartesianPoint
def LonLat_to_Cartesian(point):
	latitude_radians = math.radians(point.latitude)
	longtitude_radians = math.radians(point.longtitude)
	x = float(RADIUS * math.cos(latitude_radians) * math.cos(longtitude_radians))
	y = float(RADIUS * math.cos(latitude_radians) * math.sin(longtitude_radians))
	z = float(RADIUS * math.sin(latitude_radians))
	return CartesianPoint(x, y, z)

# Convert CartesianPoint to LongLatPoint
def Cartesian_to_LonLat(point):
	longtitude = math.degrees(math.atan2(point.y, point.x))
	latitude = math.degrees(math.asin(point.z / RADIUS))
	return LonLatPoint(latitude, longtitude)

# Given two points, return a point which is the sum of two points (Cartesian)
def addPoints(p1, p2):
	# Return a Cartesian point
	return CartesianPoint(p1.x + p2.x, p1.y + p2.y, p1.z + p2.z)
def dividePoints(sumPoints, numPoints):
    result = CartesianPoint(sumPoints.x / numPoints, sumPoints.y / numPoints, sumPoints.z / numPoints)
    return Cartesian_to_LonLat(result)
# Given two pints, return the Euclidean distance of the two points (LonLatPoint)
def EuclideanDistance(point1, point2):
	newPoint1 = LonLat_to_Cartesian(point1);
	newPoint2 = LonLat_to_Cartesian(point2);
	return math.sqrt((newPoint1.x - newPoint2.x)**2 + (newPoint1.y - newPoint2.y)**2 + (newPoint1.z - newPoint2.z)**2)

# Given two points, return the great circle distance of the two (LonLatPoint)
def GreatCircleDistance (poin1, point2):
	latitude_diff = point1.latitude - point2.latitude
	longtitude_diff = point1.longtitude - point2.longtitude
	a = math.sin(latitude_diff/2)**2 + math.cos(point1.latitude) * math.cos(point2.latitude) * (sin(longtitude_diff/2)**2)
	c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
	return RADIUS * c

# given a (latitude/longitude) point and an list of current center points,
# returns the index in the array of the center closest to the given point
def closestPoint(point, center, method):
	minIndex = 0
	minDist = float('inf')
	index = -1
	# print(len(center))
	for a in center:
		temp = -1
		index = index + 1
		# Calcute the distance using the preset method
		if method == 'EuclideanDistance':
			temp = EuclideanDistance(point, center[index])
		else:
			temp = GreatCircleDistance(point, center[index])

		if temp < minDist:
			minDist = temp
			minIndex = index
	# print("index is " + str(index))
	return minIndex

# Calculate the distance change between two center list
def distance(center1, center2, method):
	if(len(center1) != len(center2)):
		print("Length of two center array is not equal")
		sys.exit(-1)
	output = 0
	for i in range(0, len(center1)):
		if method == 'EuclideanDistance':
			output = output + EuclideanDistance(center1[i], center2[i])
		else:
			output = output + GreatCircleDistance(center1[i], center2[i])
	return output

