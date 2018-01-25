import tarfile
import cv2
import numpy as np
import os
import json



def parse_folder_name():
	"parse folder names from working directory"

	folder_name_list = []

	for i in range(11,151): 
		if i < 99:         
			dir_name = "000" + str(i) + "000" + "_" + "000" + str(i+1) + "000" + "_" + str(3)
		elif i == 99:
			dir_name = "000" + str(i) + "000" + "_" + "00" + str(i+1) + "000" + "_" + str(3)
		else:
			dir_name = "00" + str(i) + "000" + "_" + "00" + str(i+1) + "000" + "_" + str(3)

		if os.path.isdir(dir_name):
			folder_name_list.append(dir_name)
	return folder_name_list

def tar_files(img_folder):
	img_path_list = []
	for each in os.listdir(img_folder):
		img_path = img_folder + '/' + each
		img_path_list.append(img_path)

	with tarfile.open(img_folder + '.tar', "w") as tar:
		for each in img_path_list:
			tar.add(each)



if __name__ == '__main__':

	folder_name_list = parse_folder_name()
	count = 0
	for each in folder_name_list:
		tar_files(each)
		count += 1
		print("total num of folder is " + str(len(folder_name_list)), "the" + str(count) + "th is finished" )



