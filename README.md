## Optimizing Content-Based Image Retrieval for Geolocation Estimation

### Introduction

The prediction of geo-graphical location at which an image is taken is drawing increasing attention in recent research. However, one major limitation of most current research is that it focuses mostly on improving the geolocation prediction performance while ignoring the problem of index size, which is helpful in saving storage space. Traditional image retrieval index reduction approach can be achieved at the cost of losing retrieval performance, e.g., by using low-level features. This research investigates how to optimize the content-based image retrieval for geolocation estimation, by reducing the large-scale image retrieval index size without losing geo-prediction performance. More specifically, it focuses on the challenge of trade-off between index size and geo-prediction performance. 

The aim of this research is to propose an approach to investigate the possibilities to reduce the index size and improve the geo-prediction performance based on [Large Scale Image Retrieval for Location Estimation](https://repository.tudelft.nl/islandora/object/uuid%3A0d09c0dc-fcb7-4598-90e0-d2a53e675cc3). To solve the research challenge, Common Concepts Removal (CCR) is proposed, which is built based on the SSD deep learning framework. In this approach we believe that some common concepts (e.g., cars, persons, buses, etc) in restricted scenario cannot contribute to the geolocation prediction performance and the index size can be considerably reduced by removing them. These kinds of common concepts exist everywhere in the city streets and look similar, which means that they can hardly contribute to the geolocation prediction performance and even harm the prediction result in some special circumstances. We manually defined eight common concepts in San Francisco and analyzed their different influence on the geolocation prediction. We implement CCR for three different geo-prediction approaches, 1-Nearest Neighbor, Geo-Visual Ranking, and Geo-Distinctive Visual Element Matching for the geo-constrained scenario-[San Francisco Landmark Dataset](https://purl.stanford.edu/vn158kj2087). The experiment results illustrate that using this approach, the index size can be reduced by 30.6% while the performance is improved by approximately 6.0%. 


### Contents

1. [Geolocation Estimation Implementation](#Geolocation_Estimation_Implementation)
2. [Common Concepts Removal](#Common_Concept_Removal)
3. [Automatically Concepts Selection](#Automatically_Concepts_Selection)


### Geolocation_Estimation_Implementation

1. Clone the project.

  ```Shell
  git clone https://github.com/liuyiran13/Optimizing_CBIR_for_Geolocation_Estimation.git
  ```
  
2. Build the project on Eclipse (Recommend Ubuntu Enviroment).

	Import Geolocation Estimation pipeline in Eclipse from workspace directory, and import all the related .jar files from Project_jars directory. 

<img src="https://github.com/liuyiran13/Optimizing_CBIR_for_Geolocation_Estimation/blob/develope/Ref_Image/project_structure.png" width="160"/> <img src="https://github.com/liuyiran13/Optimizing_CBIR_for_Geolocation_Estimation/blob/develope/Ref_Image/libraries_import.png" width="425"/> 

3. Run make_data_ForBenchMarkTest.java locally and generate all the related hashmap files.

4. Upload the locally generated files to remote server based on the command in TMM_GVM_commands.txt.

### Common_Concept_Removal

1. Install the object detection framework based on [mxnet-ssd](https://github.com/zhreshold/mxnet-ssd).

2. Replace the related files based on SSD folder.

3. cd to SSD folder and run 
  ```Shell
python Obj_detection.py --gpu 0
  ```
4. Record the detected coordinates of objects and transfer the detected areas based on code in Common_Concept_Removal folder.

5. Generate the objects removed images into tar files by running 
  ```Shell
python tar_files.py
  ```
6. Utilize the objects removed images in Geolocation_Estimation_Implementation
<img src="https://github.com/liuyiran13/Optimizing_CBIR_for_Geolocation_Estimation/blob/develope/Ref_Image/car_0.2.png" width="425"/>
