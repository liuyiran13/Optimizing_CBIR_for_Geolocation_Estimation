from PIL import Image
import cv2
import os

# load the image and show it

index = 0
for each in os.listdir('/path/to/Automatically_select_concepts/Ranking_result/'):
	img_name = '/path/to/Automatically_select_concepts/Ranking_result/' + each
	image = cv2.imread(img_name)
	(h, w) = image.shape[:2]
	print h, w
	nrows = 10
	ncols = 10

	regionX = w/nrows
	regionY = h/ncols

	totalRegions = nrows*ncols

	for i in range(0,ncols):
		for j in range(0,nrows):
			regionY1 = i * regionY
			regionY2 = (i+1) * regionY
			regionX1 = j * regionX
			regionX2 = (j+1) * regionX
			print regionY1,regionY2, regionX1,regionX2

			cropped = image[regionY1:regionY2, regionX1:regionX2]

			file_name = '/path/to/Automatically_select_concepts/segmentation_result/' + str(index) + str(i) + str(j) + '.jpg'
			cv2.imwrite(file_name, cropped)
	index = index + 1




			