import os
import glob
from math import sin, cos, sqrt, atan2, radians
import re
import shutil
from PIL import Image

coordinate_path = '/path/to/dataset_coordinate/'
query_coordinate = [37.79279709, -122.39775848]



def list_coordinates():
	coordinate_dic = {}
	for each in glob.glob(coordinate_path + '*.gps'):
		img_name = os.path.basename(each)
		# print img_name
		f = open(each, 'r')
		x = f.readlines()
		# x = re.sub("\s+", ",", str(x).strip())
		coordinate_dic[img_name] = x
	return coordinate_dic

def calculate_distance():
	# approximate radius of earth in km
	dic = list_coordinates()
	R = 6373.0
	count = 0
	for each in dic:
		lat2 = dic[each][0][:10]
		lon2 = dic[each][0][12:24]

		lat1 = radians(query_coordinate[0])
		lon1 = radians(query_coordinate[1])
		lat2 = radians(float(lat2))
		lon2 = radians(float(lon2))

		dlon = lon2 - lon1
		dlat = lat2 - lat1

		a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
		c = 2 * atan2(sqrt(a), sqrt(1 - a))

		distance = R * c
		if distance > 2:
			file_name = each[:4] + '.' + 'jpg'
			file_path = '/path/to/Automatically_select_concepts/dataset_image/' + file_name
			save_path = '/path/to/Automatically_select_concepts/distance_filter_result/' + file_name
			# shutil.copy(file_path, save_path)
			img = Image.open(file_path)
			img = img.convert("RGBA")
			img.save(save_path, "JPEG")
			print file_name

		# print("Result:", distance)
	# return distance

# def filter_distance():
if __name__ == '__main__':
	print calculate_distance()




