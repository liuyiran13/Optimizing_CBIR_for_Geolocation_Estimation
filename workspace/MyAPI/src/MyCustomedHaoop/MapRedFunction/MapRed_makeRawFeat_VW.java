package MyCustomedHaoop.MapRedFunction;


import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.Obj.Disp;
import MyAPI.imagR.Conf_ImageR;
import MyAPI.imagR.ExtractFeat;
import MyAPI.imagR.PreProcessImage;
import MyCustomedHaoop.Mapper.SelectSamples;
import MyCustomedHaoop.Partitioner.Partitioner_equalAssign;
import MyCustomedHaoop.Partitioner.Partitioner_random_sameKey;
import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_1key_1value;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.PhotoAllFeats;

public class MapRed_makeRawFeat_VW extends Configured implements Tool{

	/**
	 * extract raw locat feature: x,y,scale,angle,descriptor, and assign VW
	 * @command_example: 
	 * SURF:
	 * Herve: 		yarn jar MapRed_makeRawFeat_VW.jar MyCustomedHaoop.MapRedFunction.MapRed_makeRawFeat_VW -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW100k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW100k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW100k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW100k_I200.ArrayList_HashSet -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisIntensityNorm=false -DmultiAssFlag=false ImageR/BenchMark/Herve/HerverImage.seq ImageR/BenchMark/Herve/feats/SURF_VW100k_SA_Herve_1.5K 30 100000
	 * Oxford: 		yarn jar MapRed_makeRawFeat_VW.jar MyCustomedHaoop.MapRedFunction.MapRed_makeRawFeat_VW -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW100k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW100k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW100k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW100k_I200.ArrayList_HashSet -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisIntensityNorm=false -DmultiAssFlag=false ImageR/BenchMark/Oxford/OxfordBuilding_cutQ.seq ImageR/BenchMark/Oxford/feats/SURF_VW100k_SA_Oxford_5K_CutQ 100 100000
	 * Barcelona: 	yarn jar MapRed_makeRawFeat_VW.jar MyCustomedHaoop.MapRedFunction.MapRed_makeRawFeat_VW -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW100k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW100k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW100k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW100k_I200.ArrayList_HashSet -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisIntensityNorm=false -DmultiAssFlag=false ImageR/BenchMark/Barcelona/Barcelona1K.seq ImageR/BenchMark/Barcelona/feats/SURF_VW100k_SA_Barcelona_1K 30 100000
	 * UniDist10M: 	yarn jar MapRed_makeRawFeat_VW.jar MyCustomedHaoop.MapRedFunction.MapRed_makeRawFeat_VW -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW100k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW100k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW100k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW100k_I200.ArrayList_HashSet -DSelPhotos=ImageR/BenchMark/UniDistractors_10M_fromFlickr66MWithPatch.hashSet -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisIntensityNorm=false -DmultiAssFlag=false 66M_Phos_Seqs ImageR/BenchMark/SURFFeat_VW100k_SA_CVPR15UniDistra_10M_Inter100K 10000 100000
	 * SanFran_Q: 	yarn jar MapRed_makeRawFeat_VW.jar MyCustomedHaoop.MapRedFunction.MapRed_makeRawFeat_VW -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelPhotos=ImageR/BenchMark/SanFrancisco/SanFrancisco_querys_transIndex_L_to_S.hashMap -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisIntensityNorm=false -DmultiAssFlag=true@10@1.2@0.05 ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data ImageR/BenchMark/SanFrancisco/feats/SURF_VW20k_MA_SanFran_Q 30 100000
	 * SanFran_D: 	yarn jar MapRed_makeRawFeat_VW.jar MyCustomedHaoop.MapRedFunction.MapRed_makeRawFeat_VW -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelPhotos=ImageR/BenchMark/SanFrancisco/SanFrancisco_docsPCI_transIndex_L_to_S.hashMap -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisIntensityNorm=false -DmultiAssFlag=false ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data ImageR/BenchMark/SanFrancisco/feats/SURF_VW20k_SA_SanFran_DPCI 1000 100000
	 * ME13_Q:		yarn jar MapRed_makeRawFeat_VW.jar MyCustomedHaoop.MapRedFunction.MapRed_makeRawFeat_VW -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelPhotos=MediaEval13/MEval13_L_to_S_test.hashMap -DselectSampleMaxS=8801049 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisIntensityNorm=false -DmultiAssFlag=true@10@1.2@0.05 66M_Phos_Seqs MM15/ImageR/SURF_VW20k_MA_ME13_Q 700 100000
	 * ME13_D:		yarn jar MapRed_makeRawFeat_VW.jar MyCustomedHaoop.MapRedFunction.MapRed_makeRawFeat_VW -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelPhotos=MediaEval13/MEval13_L_to_S_train.hashMap -DselectSampleMaxS=8539049 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisIntensityNorm=false -DmultiAssFlag=false 66M_Phos_Seqs MM15/ImageR/SURF_VW20k_SA_ME13_D 10000 100000
	 * ME15_Q:		yarn jar MapRed_makeRawFeat_VW.jar MyCustomedHaoop.MapRedFunction.MapRed_makeRawFeat_VW -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelPhotos=MediaEval15/ME15_photos_L_to_S_test.hashMap -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisIntensityNorm=false -DmultiAssFlag=true@10@1.2@0.05 Webscope100M/ME15,Webscope100M/ME14_Crawl/Photos MediaEval15/feats/SURF_VW20k_MA_ME15_Q 1000 100000
	 * ME15_D:		yarn jar MapRed_makeRawFeat_VW.jar MyCustomedHaoop.MapRedFunction.MapRed_makeRawFeat_VW -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelPhotos=MediaEval15/ME15_photos_L_to_S_train.hashMap -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisIntensityNorm=false -DmultiAssFlag=false Webscope100M/ME14_Crawl/Photos MediaEval15/feats/SURF_VW20k_SA_ME15_D 5000 100000
	 * 
	 * SIFT:
	 * SanFran_Q: 	yarn jar MapRed_makeRawFeat_VW.jar MyCustomedHaoop.MapRedFunction.MapRed_makeRawFeat_VW -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DvwCenters=ImageR/BenchMark/SanFrancisco/forVW/SIFT-binTool-UPRightINRIA2_SanFranQDPCI_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix128-128 -DHEThreshold=ImageR/BenchMark/SanFrancisco/forVW/SIFT-binTool-UPRightINRIA2_SanFranQDPCI_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr128_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/BenchMark/SanFrancisco/forVW/MiddleNode1000_onVW20kUPRightSIFTINRIA2_maxLoop200/part-r-00000 -DnodeLink_learned=ImageR/BenchMark/SanFrancisco/forVW/D_SanFranQDPCI-RFea5000-RPho100k_node_vw_links_M1000_VW20kUPRightSIFTINRIA2_I200.ArrayList_HashSet -DSelPhotos=ImageR/BenchMark/SanFrancisco/SanFrancisco_querys_transIndex_L_to_S.hashMap -DtargetFeature=SIFT-binTool-UPRightINRIA2 -DBinTool_SIFT=ImageR/forVW/SIFT_INRIA2/INRIA2_compute_descriptors_linux64 -DtargetImgSize=786432 -DisIntensityNorm=true -DmultiAssFlag=true@10@1.2@0.05 ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data ImageR/BenchMark/SanFrancisco/feats/SIFTUPRightINRIA2_QDPCIVW20k_MA_SanFran_Q 30 100000
	 * SanFran_D: 	yarn jar MapRed_makeRawFeat_VW.jar MyCustomedHaoop.MapRedFunction.MapRed_makeRawFeat_VW -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DvwCenters=ImageR/BenchMark/SanFrancisco/forVW/SIFT-binTool-UPRightINRIA2_SanFranQDPCI_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix128-128 -DHEThreshold=ImageR/BenchMark/SanFrancisco/forVW/SIFT-binTool-UPRightINRIA2_SanFranQDPCI_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr128_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/BenchMark/SanFrancisco/forVW/MiddleNode1000_onVW20kUPRightSIFTINRIA2_maxLoop200/part-r-00000 -DnodeLink_learned=ImageR/BenchMark/SanFrancisco/forVW/D_SanFranQDPCI-RFea5000-RPho100k_node_vw_links_M1000_VW20kUPRightSIFTINRIA2_I200.ArrayList_HashSet -DSelPhotos=ImageR/BenchMark/SanFrancisco/SanFrancisco_docsPCI_transIndex_L_to_S.hashMap -DtargetFeature=SIFT-binTool-UPRightINRIA2 -DBinTool_SIFT=ImageR/forVW/SIFT_INRIA2/INRIA2_compute_descriptors_linux64 -DtargetImgSize=786432 -DisIntensityNorm=true -DmultiAssFlag=false ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data ImageR/BenchMark/SanFrancisco/feats/SIFTUPRightINRIA2_QDPCIVW20k_SA_SanFran_DPCI 1000 100000
	 * 
	 */

	public static final String hdfs_address="hdfs://head02.hathi.surfsara.nl/user/yliu/"; //hdfs://p-head03.alley.sara.nl/, hdfs://head02.hathi.surfsara.nl/

	public static void main(String[] args) throws Exception {
		//prepare on local machine
//		prepareData();
		//run hadoop
		int ret = ToolRunner.run(new MapRed_makeRawFeat_VW(), args);
		System.exit(ret);
	}
	
	public static void prepareData() throws ClassNotFoundException, IOException, InterruptedException{
		SelectSamples selectSamples=new SelectSamples("/home/yiran/Desktop/Sanfrancisco/SanFrancisco_docsPCI_transIndex_L_to_S.hashMap", false);
		selectSamples.getMaxS(null, null, true);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf(); 
		String imagesPath=args[0]; //input path
		String outPath=args[1]; //out path
		int redNum_extractFeat=Integer.valueOf(args[2]);//redNum_extractFeat for extract feat, taking care of computing time
		int reducerInter_groupFeat=Integer.valueOf(args[3]);//reducer inter for group feat into MapFiles, taking care of tranfer reliability and MapFileReader number
		runHadoop(conf, imagesPath, outPath, conf.get("SelPhotos"), redNum_extractFeat, reducerInter_groupFeat);
		return 0;
	}
	
	@SuppressWarnings("rawtypes")
	public static void runHadoop(Configuration conf, String imagesPath, String outPath, 
			String selectPhotos, int redNum_extractFeat, int reducerInter_groupFeat) throws Exception {
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		//set imagesPath
		System.out.println("imagesPath:"+imagesPath);
		ArrayList<Path> imageSeqPaths = General_Hadoop.addImgPathsFromMyDataSet(imagesPath);
		System.out.println("imageSeqPaths:"+imageSeqPaths);
		//set out path
		System.out.println("save mapFile to:"+outPath);
				
		//set common Distributed cache, add selectPhotosPath
		if (conf.get("QueryPos_HashMap")!=null) {//queries from Oxford has bounding box
			cacheFilePaths.add(conf.get("QueryPos_HashMap")+"#PhotoPos_HashMap.file"); //QueryPos_HashMap with symLink
		}
		if(conf.get("targetFeature").startsWith("SIFT-binTool")){
			cacheFilePaths.add(conf.get("BinTool_SIFT")+"#BinTool_SIFT.exe"); //BinTool_SIFT path with symLink
			General_Hadoop.addToCacheListWithOriNameAsSymLink(cacheFilePaths, conf.get("BinTool_libs"), ",", "");//libs without symLink, needs keep original name
		}
		cacheFilePaths.add(conf.get("vwCenters")+"#centers.file"); //VWs path with symLink
		cacheFilePaths.add(conf.get("pMatrix")+"#pMatrix.file"); //VWs path with symLink
		cacheFilePaths.add(conf.get("HEThreshold")+"#HEThreshold.file"); //VWs path with symLink
		cacheFilePaths.add(conf.get("middleNode")+"#middleNodes.file"); //VWs path with symLink
		cacheFilePaths.add(conf.get("nodeLink_learned")+"#nodeLink_learned.file"); //VWs path with symLink
		//run 
		SelectSamples selectSamples=new SelectSamples(selectPhotos, false);
		selectSamples.addDistriCache_SelectSamples(cacheFilePaths);//SelSamples path with symLink
		String job1_out=outPath+"_temp";
		System.out.println("redNum_extractFeat for Job1: "+redNum_extractFeat);
		//******* 1st job: extract feat from photos ******
		General_Hadoop.Job(conf, imageSeqPaths.toArray(new Path[0]), job1_out, "getRawFeats", redNum_extractFeat, 8, 10, true,
				MapRed_makeRawFeat_VW.class, selectSamples.getMapper(), Partitioner_random_sameKey.class, null, null, Reducer_ExtracRawFeat_VW.class,
				IntWritable.class, BufferedImage_jpg.class, IntWritable.class,PhotoAllFeats.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 100*1024*1024L, 2,
				cacheFilePaths.toArray(new String[0]),null);
		//******* 2st job: save feats into MapFile ******
		if (reducerInter_groupFeat>0) {
			int job1RedNum_saveMapFile=1; Class partitioner=null; //noSelection, no equalAssign
			if (selectSamples.isSelection) {
				conf.set("reducerInter", reducerInter_groupFeat+"");
				Partitioner_equalAssign partitioner_equalAssign=new Partitioner_equalAssign(conf,false);
				job1RedNum_saveMapFile=partitioner_equalAssign.getReducerNum(selectSamples.getMaxS(conf.get("selectSampleMaxS"), FileSystem.get(conf), true));
				partitioner=partitioner_equalAssign.getPartitioner();
			}
			General_Hadoop.Job(conf, new Path[]{new Path(job1_out)}, outPath, "groupRawFeats", job1RedNum_saveMapFile, 8, 10, true,
					MapRed_makeRawFeat_VW.class, null, partitioner, null, null, Reducer_InOut_1key_1value.class,
					IntWritable.class, PhotoAllFeats.class, IntWritable.class,PhotoAllFeats.class,
					SequenceFileInputFormat.class, MapFileOutputFormat.class, 1*1024*1024*1024L, 2,
					null,null);
			FileSystem.get(conf).delete(new Path(job1_out), true); //delete job1_out
		}else{
			General.runSysCommand(Arrays.asList("hdfs", "dfs", "-mv",
					job1_out, outPath), null, true);
		}
	}
	
	//******** job1_1 **************	
	public static class Reducer_ExtracRawFeat_VW extends Reducer<IntWritable,BufferedImage_jpg,IntWritable,PhotoAllFeats>{
		//Reducer_extractSURF: extract double[][] feats, and SURFfeat_noSig[]
		private PreProcessImage preProcImage;
		private ExtractFeat extractFeat;
		private boolean disp;
		private int imgPhotos;
		private int noImgPhotos;
		private int noFeatPhotos;
		private int totFeatNum;
		private int totVWNum;
		private int dispInter;
		private long startTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			Conf_ImageR conf_ImageR=new Conf_ImageR(conf);
			//check file in distributted cache
			General.checkDir(new Disp(true, "", null), ".");
			//setup PreProcessImage
			preProcImage=new PreProcessImage(new Disp(true, "\t", null), conf_ImageR);
			//setup extractFeat 
			extractFeat=new ExtractFeat(new Disp(true, "\t", null), conf_ImageR);
			System.out.println("current memory:"+General.memoryInfo());
			//set procPhotos
			imgPhotos=0;
			noImgPhotos=0;
			noFeatPhotos=0;
			totFeatNum=0;
			totVWNum=0;
			//set dispInter
			dispInter=30;
			startTime=System.currentTimeMillis(); //startTime
			System.out.println("reducer setup finsihed!");
			disp=true; 
			
	 	}
		
		@Override
		protected void reduce(IntWritable key, Iterable<BufferedImage_jpg> value, Context context) throws IOException, InterruptedException {
			//key: photoName
			//value: file content
			int photoName=key.get();// photoName
			//******** only one in value! ************	
			BufferedImage_jpg photo=General_Hadoop.readOnlyOneElement(value, photoName+"");
			//get BufferedImage
			BufferedImage img=photo.getBufferedImage("photoName:"+photoName, new Disp(true, "getImageMessage: ",null));
			//process image
			if (img!=null) {
				//disp
				imgPhotos++;
	    		if((imgPhotos)%dispInter==0){ 							
					System.out.println( "extractSURF photo feat, "+imgPhotos+" photos finished!! ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
					disp=true;
				}
	    		//cut and resize image
				BufferedImage reSizedImg=preProcImage.preProcImage(img, photoName, new Disp(disp,"",null));
				//***classify visual feat to visual word***//
				PhotoAllFeats outFeats=extractFeat.extractRawFeat_makePhotoAllFeats(photoName+"", reSizedImg, disp);
				if(outFeats!=null){ // photo has feat(some photos are too small, do not have interest point)
					//output
					context.write(key, outFeats);
					if (disp==true){ 
			        	System.out.println("\t extract raw feat and make VW_Sigs for one Photo finished! photoName: "+photoName+",  outFeats: "+outFeats+", "+General.dispTime(System.currentTimeMillis()-startTime, "s"));
			        	disp=false;
					}
			        totFeatNum+=outFeats.feats.length;
			        totVWNum+=outFeats.getTotVWNum();
				}else{
					noFeatPhotos++;
					System.err.println("image exist, but no feat for photo: "+photoName);
					return;
				}
			}else {
				noImgPhotos++;
			}
			
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("\n one reducer finished! in this reducer, imaged photos: "+imgPhotos+", noImgPhotos: "+noImgPhotos+", noFeatPhotos:"+noFeatPhotos+", totFeatNum:"+totFeatNum+", totVWNum:"+totVWNum+", on average "+(float)totVWNum/totFeatNum+" vws per feat ....."+ General.dispTime(System.currentTimeMillis()-startTime, "min"));
			
	 	}
	}

}
