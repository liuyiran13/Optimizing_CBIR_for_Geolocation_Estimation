# import the necessary packages
from colordescriptor import ColorDescriptor
from searcher import Searcher
import argparse
import cv2
import os
from PIL import Image
import json
 
# construct the argument parser and parse the arguments
# ap = argparse.ArgumentParser()
# ap.add_argument("-i", "--index", required = True,
# 	help = "Path to where the computed index will be stored")
# ap.add_argument("-q", "--query", required = True,
# 	help = "Path to the query image")
# ap.add_argument("-r", "--result-path", required = True,
# 	help = "Path to the result path")
# args = vars(ap.parse_args())
 


query_path = '/home/yiran/Desktop/Automatically_select_concepts/segmentation_query/'
seg_path = '/home/yiran/Desktop/Automatically_select_concepts/segmentation_result/'
# load the query image and describe it
result = {}
for each in os.listdir(query_path):
	count = 0
	# initialize the image descriptor
	cd = ColorDescriptor((8, 12, 3))
	query = cv2.imread(query_path + each)
	features = cd.describe(query)

	# perform the search
	searcher = Searcher('/home/yiran/Desktop/Automatically_select_concepts/index.csv')
	results = searcher.search(features)

	for (score, resultID) in results:
		if score > 7:
			count = count + 1
	result[each] = count
	print(each)
	with open('Concept_Sel_Result.txt', 'w') as file:
		file.write(json.dumps(result))



 
# display the query
# cv2.imshow("Query", query)
 
# # loop over the results
# file_path = '/home/yiran/Desktop/Automatically_select_concepts/dataset_image/'
# save_path = '/home/yiran/Desktop/Automatically_select_concepts/Ranking_result/'

# for (score, resultID) in results:
# 	# load the result image and display it
# 	result = cv2.imread(args["result_path"] + "/" + resultID)
# 	print score, resultID
# 	img = Image.open(file_path + resultID)
# 	img = img.convert("RGBA")
# 	img.save(save_path+resultID, "JPEG")
# 	# savedImage = cv2.imwrite(save_path+resultID,resultID)
# 	# cv2.imshow("Result", result)
# 	# cv2.waitKey(0)