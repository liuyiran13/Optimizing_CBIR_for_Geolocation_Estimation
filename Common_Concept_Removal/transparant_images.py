import cv2
import numpy as np
import os
import json
from PIL import Image
from PIL import TarIO


def parse_folder_name():
	"parse folder names from working directory"

	working_dir = '/path/to/San_tar/'
	folder_name_list = []

	for i in range(41,151): 
		if i < 99:         
			dir_name = "000" + str(i) + "000" + "_" + "000" + str(i+1) + "000" + "_" + str(3)
		elif i == 99:
			dir_name = "000" + str(i) + "000" + "_" + "00" + str(i+1) + "000" + "_" + str(3)
		else:
			dir_name = "00" + str(i) + "000" + "_" + "00" + str(i+1) + "000" + "_" + str(3)
		folder_name = working_dir + dir_name

		if os.path.isdir(folder_name):
			folder_name_list.append(folder_name)
	return folder_name_list


def save_imgInFolder_tolist(source_folder):
	img_path_list = []
	for each in os.listdir(source_folder):
		path_name = source_folder + each
		img_path_list.append(path_name)
	return img_path_list



def transparant_img(source_folder, output_folder):
	"turning image to transparant according to json file"
	json_content = json.loads(open('/path/to/detection_result.json').read())

	if os.path.isdir(output_folder) == 0:
		os.makedirs(output_folder)

	img_path_list = save_imgInFolder_tolist(source_folder)

	count = 0
	for each in img_path_list:
		img_name = os.path.basename(each)
		if img_name in json_content:
			img = Image.open(each)
			img = img.convert("RGBA")
			pixdata = img.load()
			objects = json_content[img_name]
			if objects:
				"deal with image"
				for item in objects:
					coordinates = objects[item]
					for x in range(coordinates[0],coordinates[2]):
						for y in range(coordinates[1],coordinates[3]):
							pixdata[x,y] = (255,255,255,0)
				img.save(output_folder + img_name, "JPEG")
			else:
				img.save(output_folder + img_name, "JPEG")
			count += 1
			print("total img num is: " + str(len(os.listdir(source_folder))), "the " + str(count) + "th dealing is finised")
		else:
			img = Image.open(each)
			img.save(output_folder + img_name, "JPEG")



if __name__ == '__main__':

	save_path = '/path/to/save/folder/'
	folder_name_list = parse_folder_name()
	for each in folder_name_list:
		folder_name = os.path.basename(each)
		transparant_img(each + '/', save_path + folder_name + '/')





