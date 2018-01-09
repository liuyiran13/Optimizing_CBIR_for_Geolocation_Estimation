package SURFVW;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.General_Hadoop;
import MyAPI.Obj.Statistics;
import MyAPI.imagR.Conf_ImageR;
import MyAPI.imagR.TVector_Hadoop;
import MyCustomedHaoop.Combiner.Combiner_combine_IntArr_SURFfeat_ShortArr_Arr;
import MyCustomedHaoop.MapRedFunction.MapRed_SelectSample;
import MyCustomedHaoop.MapRedFunction.MapRed_countDataNum;
import MyCustomedHaoop.Partitioner.Partitioner_equalAssign;
import MyCustomedHaoop.ValueClass.ComposeWritableClassCollection.IntArr_SURFfeat_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.DocInfo;
import MyCustomedHaoop.ValueClass.IntArr;
import MyCustomedHaoop.ValueClass.PhotoAllFeats;
import MyCustomedHaoop.ValueClass.SURFfeat;
import MyCustomedHaoop.ValueClass.SURFfeat_ShortArr_AggSig;

public class MapRed_IndexPhotoFeat extends Configured implements Tool{

	/**compare with MapRed_IndexPhotoFeat_3jobs, combine job1 and job3 into Job1! 
	 * 
	 * 
	 * job1:  	a. compute inverted file list, term-vector, one visual world, <one photoName list + one HESig list>
	 * 			b. compute descriptor number for each photo
	 * mapper: read photo and extract SURF features, then classify each of them to visual word
	 * 			and assign visual world to HE signature
	 * 			two output for one photo: 	a. visual word, <photoName+HESig> 
	 * 										b. -1, <docInfo>
	 * partitioner: (0~job1RedNum_vwTVector-1)-th for TVector, job1RedNum_vwTVector-th for photFeatNum, no output!
	 * reducer: a. job1RedNum_vwTVector reducers, accept mapper.a output, save each 20 visual word's TVector into MapFile. (total 20K vws)
	 * 			b. 1 reducer, accept mapper.b output, save photo's descriptor number into an Array.
	 * @param 		"VWPath"  "pMatrixPath"  "HEThresholdPath" "middleNode" "nodeLink_learned" "selectPhotosPath" "docInfoPath"
	 * 
	 * job2: compute vw TVector-Length, this is used for make_vwPartitionerIDs for assign reducers when build rank
	 * mapper: read TVector file, only use job1 output Mapfile's data as input (Sequecne file)
	 * reducer: only 1 reducer, save each vw's TVector-length into a HashMap.
	 * @param    "TVectorInfoPath"
	 * 
	 * 
	 * @throws Exception 
	 * 
	 * SURF(my own kmean VW):
	 * MEva13: 	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -Dvw_num=20000 -DtaskLabel=_MEva13_9M_SURF MM15/ImageR/SURFFeat_VW20k_SA_ME13_D MM15/ImageR/
	 * Herve:	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -Dvw_num=20000 -DselectPhotosPath=ImageR/BenchMark/Herve/Herve_10M_SelPhos_L_to_S.hashMap -DtaskLabel=_Herve_10M_SURF ImageR/BenchMark/Herve/SURF/SURFFeat_VW100k_SA_Herve_1.5K,ImageR/BenchMark/SURFFeat_VW100k_SA_CVPR15UniDistra_10M_Inter100K ImageR/BenchMark/Herve/CVPR15/
	 * SanFran:	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -Dvw_num=20000 -DtaskLabel=_SanFran_DPCI_SURF ImageR/BenchMark/SanFrancisco/feats/SURF_VW20k_SA_SanFran_DPCI ImageR/BenchMark/SanFrancisco/index/
	 * ME15:	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -Dvw_num=20000 -DtaskLabel=_ME15_SURF MediaEval15/feats/SURF_VW20k_SA_ME15_D MediaEval15/index/
	 * 
	 * SIFT:
	 * SanFran:	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -Dvw_num=20000 -DtaskLabel=_SanFran_DPCI_QDPCIVW_SIFTUPRightINRIA2 ImageR/BenchMark/SanFrancisco/feats/SIFTUPRightINRIA2_QDPCIVW20k_SA_SanFran_DPCI ImageR/BenchMark/SanFrancisco/index/
	 * 
	 * Herve:	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DselectPhotosPath=ImageR/BenchMark/Herve/Herve_ori1.5K_SelPhos_L_to_S.hashMap -DtaskLabel=_Herve_1.5K -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 200 ImageR/BenchMark/Herve/HerverImage.seq ImageR/BenchMark/Herve/SURF/
	 * Oxford:  yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DselectPhotosPath=ImageR/BenchMark/Oxford/Oxford_ori5K_SelPhos_L_to_S.hashMap -DtaskLabel=_Oxford_5K -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 200 ImageR/BenchMark/Oxford/OxfordBuilding.seq ImageR/BenchMark/Oxford/SURF/
	 * Oxford:  yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DselectPhotosPath=ImageR/BenchMark/Oxford/Oxford_ori5K_SelPhos_L_to_S.hashMap -DtaskLabel=_Oxford_5K -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 200 ImageR/BenchMark/Oxford/OxfordBuilding_cutQ.seq ImageR/BenchMark/Oxford/SURF_AllCutQ/
	 * Barceln: yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DselectPhotosPath=ImageR/BenchMark/Barcelona/Barcelona_ori1K_SelPhos_L_to_S.hashMap -DtaskLabel=_Barcelona_1K -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 200 ImageR/BenchMark/Barcelona/Barcelona1K.seq ImageR/BenchMark/Barcelona/SURF/
	 * MEva14:	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DselectPhotosPath=MediaEval14/MEval14_photos_L_to_S_train.hashMap -DtaskLabel=_MEva14_5MPho -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 3000 Webscope100M/ME14_Crawl/Photos MediaEval14/
	 * MEva14:	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DselectPhotosPath=MediaEval14/ME14_PhoVidFrame_L_to_S_train.hashMap -DtaskLabel=_MEva14_5MPho1MFra -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 3000 Webscope100M/ME14_Crawl/Photos,MediaEval14/ME14Train_VidFrames_infInd MediaEval14/
	 * SanFran:	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DselectPhotosPath=ImageR/BenchMark/SanFrancisco/SanFrancisco_docsPCI_transIndex_L_to_S.hashMap -DtaskLabel=_SanFran -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 1000 ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data ImageR/BenchMark/SanFrancisco/
	 * MEva13:	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DselectPhotosPath=MediaEval13/MEval13_L_to_S_train.hashMap -DtaskLabel=_MEva13_9M -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 5000 66M_Phos_Seqs/ MM15/ImageR/
	 * 
	 * SIFT:
	 * Herve:	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SIFT_Oxford2/SIFT-binTool-Oxford2_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/forVW/HE_ProjectionMatrix64-128 -DHEThreshold=ImageR/forVW/SIFT_Oxford2/SIFT-binTool-Oxford2_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/SIFT_Oxford2/MiddleNode1000_onVW20k_maxLoop200/part-r-00000 -DnodeLink_learned=ImageR/forVW/SIFT_Oxford2/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DselectPhotosPath=ImageR/BenchMark/Herve/Herve_ori1.5K_SelPhos_L_to_S.hashMap -DtaskLabel=_Herve_1.5K -DtargetFeature=SIFT-binTool-Oxford2 -DBinTool_SIFT=ImageR/forVW/SIFT_Oxford2/Oxford2_extract_features_64bit.ln -DtargetImgSize=786432 200 ImageR/BenchMark/Herve/HerverImage.seq ImageR/BenchMark/Herve/SIFT_Oxford2/
	 * Herve:	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SIFT_INRIA2/SIFT-binTool-INRIA2_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000   -DpMatrix=ImageR/forVW/HE_ProjectionMatrix64-128 -DHEThreshold=ImageR/forVW/SIFT_INRIA2/SIFT-binTool-INRIA2_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000   -DmiddleNode=ImageR/forVW/SIFT_INRIA2/MiddleNode1000_onVW20k_maxLoop200/part-r-00000  -DnodeLink_learned=ImageR/forVW/SIFT_INRIA2/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet  -DselectPhotosPath=ImageR/BenchMark/Herve/Herve_ori1.5K_SelPhos_L_to_S.hashMap -DtaskLabel=_Herve_1.5K -DtargetFeature=SIFT-binTool-INRIA2  -DBinTool_SIFT=ImageR/forVW/SIFT_INRIA2/INRIA2_compute_descriptors_linux64 -DtargetImgSize=786432 200 ImageR/BenchMark/Herve/HerverImage.seq ImageR/BenchMark/Herve/SIFT_INRIA2/
	 * Herve:	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SIFT_VLFeat/SIFT-binTool-VLFeat_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000   -DpMatrix=ImageR/forVW/HE_ProjectionMatrix64-128 -DHEThreshold=ImageR/forVW/SIFT_VLFeat/SIFT-binTool-VLFeat_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000   -DmiddleNode=ImageR/forVW/SIFT_VLFeat/MiddleNode1000_onVW20k_maxLoop200/part-r-00000  -DnodeLink_learned=ImageR/forVW/SIFT_VLFeat/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet  -DselectPhotosPath=ImageR/BenchMark/Herve/Herve_ori1.5K_SelPhos_L_to_S.hashMap -DtaskLabel=_Herve_1.5K -DtargetFeature=SIFT-binTool-VLFeat  -DBinTool_SIFT=ImageR/forVW/SIFT_VLFeat/VLFeat09_sift_linux64 -DBinTool_libs=ImageR/forVW/SIFT_VLFeat/libvl.so -DtargetImgSize=786432 200 ImageR/BenchMark/Herve/HerverImage.seq ImageR/BenchMark/Herve/SIFT_VLFeat/
	 * Herve:	yarn jar MapRed_IndexPhotoFeat.jar SURFVW.MapRed_IndexPhotoFeat -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SIFT_INRIA2/SIFT-binTool-INRIA2_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000   -DpMatrix=ImageR/forVW/HE_ProjectionMatrix64-128 -DHEThreshold=ImageR/forVW/SIFT_INRIA2/SIFT-binTool-INRIA2_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000   -DmiddleNode=ImageR/forVW/SIFT_INRIA2/MiddleNode1000_onVW20k_maxLoop200/part-r-00000  -DnodeLink_learned=ImageR/forVW/SIFT_INRIA2/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet  -DselectPhotosPath=ImageR/BenchMark/Herve/Oxford_ori5K_SelPhos_L_to_S.hashMap -DtaskLabel=_Oxford_5K -DtargetFeature=SIFT-binTool-INRIA2  -DBinTool_SIFT=ImageR/forVW/SIFT_INRIA2/INRIA2_compute_descriptors_linux64 -DtargetImgSize=786432 200 ImageR/BenchMark/Oxford/OxfordBuilding_cutQ.seq ImageR/BenchMark/Oxford/SIFT_INRIA2_AllCutQ/
	 */
	
	public static final String hdfs_address="hdfs://head02.hathi.surfsara.nl/user/yliu/"; //hdfs://p-head03.alley.sara.nl/, hdfs://head02.hathi.surfsara.nl/

	public static void main(String[] args) throws Exception {
//		forTest();
		
//		preparData();
		
		runHadoop(args);
		
	}
	
	public static void preparData() throws Exception {
		//***** for 3M *********//
		int totNum=3185258;
		HashMap<Integer, Integer> transIndex_LtoS= new HashMap<Integer, Integer>();
		for (int i = 1; i <= totNum; i++) {
			transIndex_LtoS.put(i, i);
		}
		General.writeObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/3M_transIndex_LtoS.hashMap", transIndex_LtoS);
	}
	
	public static void forTest() throws Exception {
//		//********* for test TVector MapFile **************//
//		Configuration conf = new Configuration();
//		FileSystem fs=FileSystem.get(conf);
//		IntWritable key=new IntWritable();
//		IntArr_byteArrArrArr_Short value = new IntArr_byteArrArrArr_Short();//<Names&Sigs>
//		MapFile.Reader MapFileReader=new MapFile.Reader(fs, "Q:\\part-r-00000", conf);
//		System.out.println(MapFileReader.getKeyClass());
//		System.out.println(MapFileReader.getValueClass());
//		while(MapFileReader.next(key, value)==true){
//			System.out.println("key in MapFile: "+key);
//		}
//		MapFileReader.close();
//		
//		//********* for test VW extraction time **************//
//		Configuration conf = new Configuration();
//		FileSystem fs=FileSystem.get(conf);
//		IntWritable key=new IntWritable();
//		BufferedImage_jpg value = new BufferedImage_jpg();//<Names&Sigs>
//		SequenceFile.Reader SeqFileReader=new SequenceFile.Reader(fs, new Path("Q:/FlickrCrawler/Photos_5.0_5.999999_seq/part-r-00000"), conf);
//		System.out.println(SeqFileReader.getKeyClass());
//		System.out.println(SeqFileReader.getValueClass());
//		//read visual word cluster centers
//		BufferedReader intstr_data = new BufferedReader(new InputStreamReader(new FileInputStream("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/SURFVW/SURFVW_20K_I90"), "UTF-8"));
//	    String line1Photo; int centerNum=0; 
//	    while((line1Photo=intstr_data.readLine())!=null){ //line1Photo: VL-99983{n=3401 c=[-34.177, 153.369, .....] r=[7.903, 14.364, .....]}
//	    	centerNum++;
//		}intstr_data.close(); 
//		double[][] centers=new double[centerNum][];
//		int lineindex=0;
//		intstr_data = new BufferedReader(new InputStreamReader(new FileInputStream("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/SURFVW/SURFVW_20K_I90"), "UTF-8"));
//		while((line1Photo=intstr_data.readLine())!=null){ //line1Photo: VL-99983{n=3401 c=[-34.177, 153.369, .....] r=[7.903, 14.364, .....]}
//			centers[lineindex]=General.StrArrToDouArr(line1Photo.split("\\[")[1].split("\\]")[0].split(","));
//			lineindex++;
//		}intstr_data.close();
//		System.out.println("visual word numbers: "+centerNum);
//		//read projection matrix P ***//
//		DenseMatrix64F pMatrix=(DenseMatrix64F) General.readObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/SURFVW/HE_ProjectionMatrix");
//		//***** read HEThreshold ***//
//		intstr_data = new BufferedReader(new InputStreamReader(new FileInputStream("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/SURFVW/HE_Thresholds"), "UTF-8"));
//		double[][] HEThreshold=new double[centerNum][];lineindex=0;
//		while((line1Photo=intstr_data.readLine())!=null){ 
//			HEThreshold[lineindex]=General.StrArrToDouArr(line1Photo.split(","));
//			lineindex++;
//		}intstr_data.close();
////		//read selected photos ***//
////		HashSet<Integer> selectedPhotos= (HashSet<Integer>) General.readObject("Q:/FlickrCrawler/MetaData/10M_selectedPhotos.hashSet_int");
////		System.out.println("selectedPhotos:"+selectedPhotos.size());
//		//read nodes ***//
//		ArrayList<double[]> nodes=(ArrayList<double[]>) General.readObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/SURFVW/middleNodes_M1000_VW20000_I200.ArrayList_HashSet");
//		//read links ***//
//		ArrayList<HashSet<Integer>> node_vw_links=(ArrayList<HashSet<Integer>>) General.readObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/SURFVW/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet");
//		int maxLength=0;  int minLength=999999; 
//		for(int i=0;i<node_vw_links.size();i++){
//			maxLength=Math.max(node_vw_links.get(i).size(),maxLength);
//			minLength=Math.min(node_vw_links.get(i).size(),minLength);
//		}
//		System.out.println("node_vw_links, link number per node, maxLength: "+maxLength+", minLength:"+minLength);
//		//*** built query photo's vw_photoName_sigs  ******//
//		long endTime, startTime; 
//		while(SeqFileReader.next(key, value)==true){
//			int photoName=key.get();// photoName
////			if(selectedPhotos.contains(photoName)){
//				startTime=System.currentTimeMillis(); //startTime
//				BufferedImage img_bufferedImage=value.getBufferedImage(); //image content
//				ImageFloat32 img=General_BoofCV.BoofCV_loadImage(img_bufferedImage,ImageFloat32.class);
//				//***classify visual feat to visual word***//
//				HashMap<Integer,ArrayList<byte[]>> VW_Sigs=new HashMap<Integer,ArrayList<byte[]>>();
//				double[][] photoFeat=General_BoofCV.computeSURF(img,"2,1,5,true","2000,1,9,4,4");
//				if(photoFeat!=null){ // photo has feat(some photos are too small, do not have interest point)
//					endTime=System.currentTimeMillis(); //end time 
//					System.out.println( "extract photo feat finished!! ......"+ General.dispTime (endTime-startTime, "ms")+", photoFeat number:"+photoFeat.length);
//					int wrongCenterNum=0; startTime=System.currentTimeMillis(); //startTime
//					for(int i=0;i<photoFeat.length;i++){
//						int centerIndex_GANN=General.assignFeatToCenter_fastGANN(photoFeat[i], centers, nodes,  node_vw_links);  //visual word index from 0
//						int centerIndex_real=General.assignFeatToCenter(photoFeat[i], centers);  //visual word index from 0
//						if(centerIndex_GANN!=centerIndex_real)
//							wrongCenterNum++;
//						BitSet HESig=General_EJML.makeHEsignature_BitSet(photoFeat[i], pMatrix, HEThreshold[centerIndex_GANN]);
//						byte[] HESig_Bytes=General.BitSettoByteArray(HESig);
//						General.Assert(HESig_Bytes.length == (HEThreshold[0].length/8), "HESig to bytes error!! Bytes.length!=(HEThreshold[0].length/8) "+photoName);
//						//add this vw-sig to HashMap
//						if (VW_Sigs.containsKey(centerIndex_GANN)){
//							VW_Sigs.get(centerIndex_GANN).add(HESig_Bytes);
//						}else{
//							ArrayList<byte[]> temp= new ArrayList<byte[]>();
//							temp.add(HESig_Bytes);
//							VW_Sigs.put(centerIndex_GANN,temp);
//						}
//					}
//					endTime=System.currentTimeMillis(); //end time 
//					System.out.println( "indexing photo feat finished!! ......"+ General.dispTime (endTime-startTime, "ms")
//							+", photoFeat number:"+photoFeat.length+", VW_Sigs key number:"+VW_Sigs.size()+", wrongCenterNum:"+wrongCenterNum);
//				}else{
//					System.err.println("image exist, but no feat for "+photoName);
//				}
////			}
//		}
//		SeqFileReader.close();
		
		//********* for test photoFeatNum **************//
		int[] photoFeatNum=(int[]) General.readObject("D:/xinchaoli/Desktop/My research/photoFeatNum_3M_test");
		int indexedPhotoNum=0;
		for (int i = 0; i < photoFeatNum.length; i++) {
			if (photoFeatNum[i]!=0) {
				indexedPhotoNum++;
			}
		}
		System.out.println("indexedPhotoNum: "+indexedPhotoNum);
	}

	public static void runHadoop(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_IndexPhotoFeat(), args);
		System.exit(ret);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem hdfs=FileSystem.get(conf); 
		String[] otherArgs = args; //use this to parse args!
		//get vw_num
	    int vw_num=Integer.valueOf(conf.get("vw_num"));
	    int VWFileInter=conf.get("VWFileInter")==null?vw_num/1000:Integer.valueOf(conf.get("VWFileInter"));//by default VWFileInter=vw_num/1000
	    conf.set("VWFileInter", VWFileInter+"");	
	    System.out.println("vw_num:"+vw_num+", VWFileInter:"+VWFileInter);
		//set taskLabel
		String taskLabel=conf.get("taskLabel")+"_VW"+vw_num/1000+"K"; 
		System.out.println("taskLabel:"+taskLabel);
		//set imagesPath
		String featPath=otherArgs[0];
		System.out.println("featPath: "+featPath);
		//set workPath
		String workPath=otherArgs[1];
		//set TVectorPath
		String TVectorPath=workPath+"TVector"+taskLabel;
		System.out.println("TVectorPath:"+TVectorPath);
		//set TVectorPath_numOnly
		String TVectorPath_numOnly=workPath+"TVectorNumOnly"+taskLabel;
		System.out.println("TVectorPath_numOnly:"+TVectorPath_numOnly);
		//set docInfoPath
		String docInfoPath=workPath+"docInfo"+taskLabel;
		//set TVectorInfoPath
		conf.set("TVectorInfo", workPath+"TVectorInfo"+taskLabel); 
		//************************************* select feats ********************************
		String featPath_sel=workPath+"temp_SelFeats"+taskLabel;
		MapRed_SelectSample.runHadoop(conf, General_Hadoop.strArr_to_PathArr(featPath.split(",")), featPath_sel, conf.get("selectPhotosPath"), IntWritable.class, PhotoAllFeats.class);
		//************************************* job1: make TVector ********************************
		conf.set("reducerInter", VWFileInter+"");
		conf.set("TVector", TVectorPath);
		String out_job1=TVectorPath+"_info";
		//set partitioner
		Partitioner_equalAssign partitioner_equalAssign=new Partitioner_equalAssign(conf,false);
		int job1_1RedNum=partitioner_equalAssign.getReducerNum(vw_num);//1 reducer, 1 folder, each folder has VWFileInter seqs
		System.out.println("total reducers for job1_1, job1_1RedNum:"+job1_1RedNum+", for 1 reducer: "+VWFileInter+" vw!");
		//run
		General_Hadoop.Job(conf, new Path[]{new Path(featPath_sel)}, out_job1, "TVector", job1_1RedNum, 8, 2, false, 
				MapRed_IndexPhotoFeat.class, Mapper_make_TVector.class, partitioner_equalAssign.getPartitioner(), Combiner_combine_IntArr_SURFfeat_ShortArr_Arr.class, null, Reducer_make_TVector.class,
				IntWritable.class, IntArr_SURFfeat_ShortArr_Arr.class, IntWritable.class, IntArr.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
				null,null);
		//************************************ job2: make TVectorInfo *************************************
		General_Hadoop.Job(conf, new Path[]{new Path(out_job1)}, null, "TVectorInfo_save", 1, 8, 2, false,
				MapRed_IndexPhotoFeat.class, null, null,null,null,Reducer_saveTVectorLength.class,
				IntWritable.class, IntArr.class, IntWritable.class,IntWritable.class,
				SequenceFileInputFormat.class, NullOutputFormat.class, 1*1024*1024*1024L, 0,
				null,null);
		//****************************************** job3: make docInfo ********************************************
		int totDocNum=MapRed_countDataNum.runHadoop(conf, new Path[]{new Path(featPath_sel)}, workPath);
		conf.set("totDocNum", totDocNum+"");
		System.out.println("totDocNum: "+totDocNum);
		//run
		String out_Job3=docInfoPath;
		ArrayList<String> cacheFilePaths=new ArrayList<>();
		cacheFilePaths.clear();
		Conf_ImageR.addDistriCache_TVectorInfo(conf, cacheFilePaths); //TVectorInfo with symLink
		General_Hadoop.Job(conf, new Path[]{new Path(featPath_sel)}, out_Job3, "docInfo", 1, 8, 2, true,
				MapRed_IndexPhotoFeat.class, Mapper_make_docInfo.class, null, null,null, Reducer_make_docInfo.class,
				IntWritable.class, DocInfo.class, IntWritable.class, DocInfo.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		
		//clean-up
		hdfs.delete(new Path(featPath_sel),true);//TVector_info
		hdfs.delete(new Path(out_job1),true);//TVector_info
		hdfs.close();
		return 0;

	}
	
	//******** job1_2 **************
	public static class Mapper_make_TVector extends Mapper<IntWritable,PhotoAllFeats,IntWritable,IntArr_SURFfeat_ShortArr_Arr>{
		
		int photoNum=0;
		int dispInter=1000;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("this mapper is used to re-organise feats by vw");
			System.out.println("setup finsihed!");
	 	}
		
		@Override
		protected void map(IntWritable photoID, PhotoAllFeats feats, Context context) throws IOException, InterruptedException {
			//key: photoNames
			//value: feats
			photoNum++;
			HashMap<Integer,SURFfeat_ShortArr_AggSig> VW_Sigs=feats.group_VW_SURFfeatAggSig();
			for (Entry<Integer, SURFfeat_ShortArr_AggSig> one : VW_Sigs.entrySet()) {
				int vw=one.getKey();
				context.write(new IntWritable(vw), new IntArr_SURFfeat_ShortArr_Arr(new int[]{photoID.get()}, new SURFfeat[][]{one.getValue().feats}, new  byte[][]{one.getValue().aggSig}));
			}
			if (photoNum%dispInter==0) {
				System.out.println(photoNum+" photos finished!, current photoID:"+photoID+", feats:"+feats);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("this mapper done! "+photoNum+" photos finished!");
		}
	}
	
	public static class Reducer_make_TVector extends Reducer<IntWritable,IntArr_SURFfeat_ShortArr_Arr,IntWritable,IntArr>  {
		Conf_ImageR conf_ImageR;

		private StringBuffer vws;
		private int reduceNum;
		private int dispInter_photoNum;
		private int dispInter_reduceNum;
		private boolean disp;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf_ImageR=new Conf_ImageR(context.getConfiguration());

			vws=new StringBuffer();
			reduceNum=0;
			dispInter_photoNum=500*1000;
			dispInter_reduceNum=200;
			disp=true; 
			System.out.println("setup finsihed! disp:"+disp);
		}
		
		@Override
		public void reduce(IntWritable vw, Iterable<IntArr_SURFfeat_ShortArr_Arr> photoName_feats, Context context) throws IOException, InterruptedException {
			/**
			 * 1 vw, 1 TVector, 1 reducer, 1 seqFile
			 * SeqFile;	outputFormat: key_IntWritable-->(photoName)  value_SURFfeat_ShortArr-->(SURFfeats),  use short, so photo feats number should be 0~32767
			 * 1st element is to mark vw, value is not useful!
			 */
			reduceNum++;
			if (reduceNum%dispInter_reduceNum==0) {
				System.out.println("\n now is "+reduceNum+"-th reduce! current is for vw:"+vw+", current memory info: "+General.memoryInfo());
				disp=true;
			}
			//photoName_featNum_feats: [photoName_featNum], feats
			General.Assert(vw.get()>-1, "err in Reducer_make_TVector, all key should >-1, here="+vw.get()); //vw from 0, if vw==-1, this is for docInfo
			//*** built vw's TVector: photoName_sigs  ******//
			TVector_Hadoop tVector=new TVector_Hadoop(context.getConfiguration(), conf_ImageR.sd_TVector_HDFSPath, conf_ImageR.sd_VWFileInter,disp);
			int photoNum=0; int featNum=0; long startTime=System.currentTimeMillis();
			for(IntArr_SURFfeat_ShortArr_Arr one:photoName_feats){
				int this_photoNum=one.obj_1.getIntArr().length;
				for(int i=0; i<this_photoNum; i++){
					int photoName=one.obj_1.getIntArr()[i];//0:photoName, 1: featNum, not used for vw TVector
					SURFfeat_ShortArr_AggSig featArr=one.obj_2.getArrArr()[i];
					tVector.tVector.addOneDoc(photoName, featArr.feats, featArr.aggSig);
					//check featNum, photoNum
					featNum+=featArr.feats.length;
					photoNum++;
					if (disp==true && photoNum%dispInter_photoNum==0) {
						System.out.println("\t --- "+photoNum+" photos and "+featNum+" feats finished! time: "+General.dispTime(System.currentTimeMillis()-startTime, "s"));
					}
				}
	        }
			if(disp==true){
				System.out.println("read this vw's docs-feats into memory finished! docNum: "+ photoNum+", featNum: "+featNum
						+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s")+", current memory info: "+General.memoryInfo());
			}
			//out-put TVector
			tVector.makeTVector(vw.get(), disp);
			//out-put TVector-info
			context.write(new IntWritable(1), new IntArr(new int[]{vw.get(),photoNum,featNum}));	
			vws.append(vw.get()+"_"+photoNum+"_"+featNum+", ");
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("\n one reducer finished! total "+reduceNum+" vws in this Reducer, vw_photoNum_featNum: "+vws.toString());
		}
	}
	
	//******** job1_3 **************
	public static class Mapper_make_docInfo extends Mapper<IntWritable,PhotoAllFeats,IntWritable,DocInfo>{
		
		float[] idf_squre;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();	
			int[][] TVectorInfo=(int[][]) General.readObject(Conf_ImageR.sd_TVectorInfo);
			int totDocNum=Integer.valueOf(conf.get("totDocNum"));
			idf_squre=General_BoofCV.make_idf_squre(TVectorInfo, totDocNum);
			System.out.println("this mapper is used to make docInfo, idf_squre is calculated!");
			System.out.println("setup finsihed!");
	 	}
		
		@Override
		protected void map(IntWritable key, PhotoAllFeats value, Context context) throws IOException, InterruptedException {
			//key: photoNames
			//value: feats
			context.write(key, value.getDocInfo(idf_squre));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
		}
	}
	
	public static class Reducer_make_docInfo extends Reducer<IntWritable,DocInfo,IntWritable,DocInfo>  {
		Statistics<Integer> stat;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			stat=new Statistics<>(10);
		}
		
		@Override
		public void reduce(IntWritable photoName, Iterable<DocInfo> value, Context context) throws IOException, InterruptedException {
			/**
			 * only 1 reducer, save all docs info
			 */
			DocInfo docInfo = General_Hadoop.readOnlyOneElement(value, photoName+"");
			stat.addSample(docInfo.pointNum, photoName.get());
			context.write(photoName, docInfo);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("only 1 reducer, finished! docFeatNum stat: "+stat.getFullStatistics("0", false));
		}
	}
	
	//******** job2 **************
	
	public static class Reducer_saveTVectorLength extends Reducer<IntWritable,IntArr,IntWritable,IntWritable>  {
		
		private Configuration conf;
		private String TVectorInfoPath;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf=context.getConfiguration();
			Conf_ImageR conf_ImageR=new Conf_ImageR(conf);
			//***** read save path for HashMap:vw_TVectorLength ***//
			TVectorInfoPath = conf_ImageR.sd_TVectorInfo_HDFSPath;
			System.out.println("TVectorInfoPath setted, "+TVectorInfoPath);
			
			System.out.println("setup finsihed!");
			
	 	}
		
		@Override
		public void reduce(IntWritable only1reducer, Iterable<IntArr> vw_TVLength, Context context) throws IOException, InterruptedException {
			
			FileSystem fs=FileSystem.get(conf);
			int vw_num=Integer.valueOf(conf.get("vw_num"));
			
			int[][] vw_TVectorInfo=new int[vw_num][]; //vw index should be from 0 !	
			int ind=0; int dispInter=10000;
			Statistics<Integer> stat_photoNum=new Statistics<>(10); Statistics<Integer> stat_featNum=new Statistics<>(10);
			for(Iterator<IntArr> it=vw_TVLength.iterator();it.hasNext();){ //only 1 element
				int[] one =it.next().getIntArr();
				int vw=one[0];
				int photoNum=one[1];
				int featNum=one[2];
				vw_TVectorInfo[vw]=new int[]{photoNum,featNum};
				 //debug disp info
				ind++;
				if (ind%dispInter==0){
					System.out.println(ind+"-th vw, vw: "+vw+", photoNum: "+photoNum+", featNum:"+featNum);
				}
				stat_photoNum.addSample(photoNum, vw);
				stat_featNum.addSample(featNum, vw);
	        }
			
			//assert
			for (int i = 0; i < vw_TVectorInfo.length; i++) {
				if (vw_TVectorInfo[i]!=null) {
					General.Assert(vw_TVectorInfo[i][0]!=0, "vw-"+i+" in vw_TVectorInfo not null, but photoNum==0!!");
					General.Assert(vw_TVectorInfo[i][1]!=0, "vw-"+i+" in vw_TVectorInfo not null, but featNum==0!!");
				}else {//some vw do not have any photos!
					System.out.println("vw-"+i+" in vw_TVectorInfo is null!!");
				}				
			}
			
			General_Hadoop.writeObject_HDFS(fs, TVectorInfoPath, vw_TVectorInfo);
			System.out.println("reducer finished! no output, only save vw_TVectorInfo to "+TVectorInfoPath);
			System.out.println("total processed vws:"+vw_TVectorInfo.length);
			System.out.println("vws stat_photoNum:"+stat_photoNum.getFullStatistics("0", false));
			System.out.println("vws stat_featNum:"+stat_featNum.getFullStatistics("0", false));
		}

	}

}
