package SURFVW;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.MapRedFunction.MapRed_KMean;
import MyCustomedHaoop.Mapper.SelectSamples;
import MyCustomedHaoop.Partitioner.Partitioner_random;
import MyCustomedHaoop.Reducer.Reducer_RawLocalFeature;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.FloatArr;
import MyCustomedHaoop.ValueClass.IntArr;

public class MapRed_makeGANN extends Configured implements Tool{

	/**
	 * main: generate middle cluster nodes, using kMean on 20k visual worlds
	 * 
	 * job1: find link between middle nodes and real visual worlds
	 * mapper: read photo and extract SURF features, then classify each of them to middle nodes and real visual word
	 * 			build link between them 
	 * 			output: middle node, real visual worlds which has link to this middle node
	 * reducer: 1 reducer, combine each node's link, save to ArrayList
	 * 	@param 	"VWPath"	"randomFeatNum" "nodeLink_learned"
	 * 
	 * 
	 * @throws Exception 
	 * 
	 * SURF:
	 * Generic:		yarn jar MapRed_makeGANN.jar SURFVW.MapRed_makeGANN -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar -DrandomPhoNum_forLearnNodeLink=100000 -DrandomPhoNum_forTestNodeLink=50000 -DdataLabel_forLearnNodeLink=_MedEval3M@1_3185258 -DdataLabel_forTestNodeLink=_66M@3185259_77000000 -DrandFeatNum_forLearnNodeLink=3000 -DrandFeatNum_forTestNodeLink=3000 -DmiddleNodeNum=1000 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=0 ImageR/forVW/D_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000  ImageR/forVW/ 200 20k  4000 3M_Photos_SeqFiles 66M_Phos_Seqs
	 * Generic:		yarn jar MapRed_makeGANN.jar SURFVW.MapRed_makeGANN -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar -DrandomPhoNum_forLearnNodeLink=100000 -DrandomPhoNum_forTestNodeLink=50000 -DdataLabel_forLearnNodeLink=_MedEval3M@1_3185258 -DdataLabel_forTestNodeLink=_66M@3185259_77000000 -DrandFeatNum_forLearnNodeLink=3000 -DrandFeatNum_forTestNodeLink=3000 -DmiddleNodeNum=1000 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=0 ImageR/forVW/D_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW100k/loop-99/part-r-00000 ImageR/forVW/ 200 100k 4000 3M_Photos_SeqFiles 66M_Phos_Seqs
	 * Oxford:		yarn jar MapRed_makeGANN.jar SURFVW.MapRed_makeGANN -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar -DselectPhotosPath=ImageR/BenchMark/Oxford/Oxford_ori5K_SelPhos_L_to_S.hashMap -DrandomPhoNum_forLearnNodeLink=5000 -DrandomPhoNum_forTestNodeLink=5000 -DdataLabel_forLearnNodeLink=_Oxford5K@noUse -DdataLabel_forTestNodeLink=_Oxford5K@noUse -DrandFeatNum_forLearnNodeLink=5000 -DrandFeatNum_forTestNodeLink=5000 -DmiddleNodeNum=1000 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=0 ImageR/BenchMark/Oxford/forVW/SURF_Oxford5K_forVW-RFea5000-RPho10k_KMean_VW20k/loop-99/part-r-00000 ImageR/BenchMark/Oxford/forVW/ 200 20k 1000 ImageR/BenchMark/Oxford/OxfordBuilding.seq ImageR/BenchMark/Oxford/OxfordBuilding.seq
	 * Holiday:		yarn jar MapRed_makeGANN.jar SURFVW.MapRed_makeGANN -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar -DselectPhotosPath=ImageR/BenchMark/Herve/Herve_ori1.5K_SelAllPhos_L_to_S.hashMap -DrandomPhoNum_forLearnNodeLink=5000 -DrandomPhoNum_forTestNodeLink=5000 -DdataLabel_forLearnNodeLink=_Herve1.5K@noUse -DdataLabel_forTestNodeLink=_Herve1.5K@noUse -DrandFeatNum_forLearnNodeLink=5000 -DrandFeatNum_forTestNodeLink=5000 -DmiddleNodeNum=1000 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=0 ImageR/BenchMark/Herve/forVW/SURF_Herve1.5K_forVW-RFea5000-RPho10k_KMean_VW20k/loop-99/part-r-00000 ImageR/BenchMark/Herve/forVW/ 200 20k 200 ImageR/BenchMark/Herve/HerverImage.seq ImageR/BenchMark/Herve/HerverImage.seq
	 * SanFran:		yarn jar MapRed_makeGANN.jar SURFVW.MapRed_makeGANN -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar -DselectPhotosPath=ImageR/BenchMark/SanFrancisco/SanFrancisco_Q-DPCI_transIndex_L_to_S.hashMap -DrandomPhoNum_forLearnNodeLink=100000 -DrandomPhoNum_forTestNodeLink=50000 -DdataLabel_forLearnNodeLink=_SanFranQDPCI@noUse -DdataLabel_forTestNodeLink=_SanFranQDPCI@noUse -DrandFeatNum_forLearnNodeLink=5000 -DrandFeatNum_forTestNodeLink=5000 -DmiddleNodeNum=1000 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=0 ImageR/BenchMark/SanFrancisco/forVW/SURF_SanFranQandPCI_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 ImageR/BenchMark/SanFrancisco/forVW/ 200 20k 4000 ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data
	 * 
	 * SIFT:
	 * hadoop jar MapRed_makeGANN.jar SURFVW.MapRed_makeGANN -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar -DrandomPhoNum_forLearnNodeLink=100000 -DrandomPhoNum_forTestNodeLink=50000 -DdataLabel_forLearnNodeLink=_MedEval3M@_1_3185258 -DdataLabel_forTestNodeLink=_66M@_3185259_77000000 -DrandFeatNum_forLearnNodeLink=3000 -DrandFeatNum_forTestNodeLink=3000 -DmiddleNodeNum=1000 -DtargetFeature=SIFT-binTool-Oxford2 -DBinTool_SIFT=ImageR/forVW/SIFT_Oxford2/Oxford2_extract_features_64bit.ln -DtargetImgSize=786432 ImageR/forVW/SIFT_Oxford2/SIFT-binTool-Oxford2_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 ImageR/forVW/SIFT_Oxford2/ 200 20k 4000 3M_Photos_SeqFiles 66M_Phos_Seqs
	 * hadoop jar MapRed_makeGANN.jar SURFVW.MapRed_makeGANN -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar -DrandomPhoNum_forLearnNodeLink=100000 -DrandomPhoNum_forTestNodeLink=50000 -DdataLabel_forLearnNodeLink=_MedEval3M@_1_3185258 -DdataLabel_forTestNodeLink=_66M@_3185259_77000000 -DrandFeatNum_forLearnNodeLink=3000 -DrandFeatNum_forTestNodeLink=3000 -DmiddleNodeNum=1000 -DtargetFeature=SIFT-binTool-INRIA2 -DBinTool_SIFT=ImageR/forVW/SIFT_INRIA2/INRIA2_compute_descriptors_linux64 -DtargetImgSize=786432 ImageR/forVW/SIFT_INRIA2/SIFT-binTool-INRIA2_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 ImageR/forVW/SIFT_INRIA2/ 200 20k 4000 3M_Photos_SeqFiles 66M_Phos_Seqs
	 * hadoop jar MapRed_makeGANN.jar SURFVW.MapRed_makeGANN -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar -DrandomPhoNum_forLearnNodeLink=100000 -DrandomPhoNum_forTestNodeLink=50000 -DdataLabel_forLearnNodeLink=_MedEval3M@_1_3185258 -DdataLabel_forTestNodeLink=_66M@_3185259_77000000 -DrandFeatNum_forLearnNodeLink=3000 -DrandFeatNum_forTestNodeLink=3000 -DmiddleNodeNum=1000 -DtargetFeature=SIFT-binTool-VLFeat -DBinTool_SIFT=ImageR/forVW/SIFT_VLFeat/VLFeat09_sift_linux64 -DBinTool_libs=ImageR/forVW/SIFT_VLFeat/libvl.so -DtargetImgSize=786432 ImageR/forVW/SIFT_VLFeat/SIFT-binTool-VLFeat_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 ImageR/forVW/SIFT_VLFeat/ 200 20k 4000 3M_Photos_SeqFiles 66M_Phos_Seqs
	 * SanFran:		yarn jar MapRed_makeGANN.jar SURFVW.MapRed_makeGANN -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DselectPhotosPath=ImageR/BenchMark/SanFrancisco/SanFrancisco_Q-DPCI_transIndex_L_to_S.hashMap -DrandomPhoNum_forLearnNodeLink=100000 -DrandomPhoNum_forTestNodeLink=50000 -DdataLabel_forLearnNodeLink=_SanFranQDPCI@noUse -DdataLabel_forTestNodeLink=_SanFranQDPCI@noUse -DrandFeatNum_forLearnNodeLink=5000 -DrandFeatNum_forTestNodeLink=5000 -DmiddleNodeNum=1000 -DtargetFeature=SIFT-binTool-UPRightINRIA2 -DBinTool_SIFT=ImageR/forVW/SIFT_INRIA2/INRIA2_compute_descriptors_linux64 -DtargetImgSize=786432 -DisIntensityNorm=true ImageR/BenchMark/SanFrancisco/forVW/SIFT-binTool-UPRightINRIA2_SanFranQDPCI_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000  ImageR/BenchMark/SanFrancisco/forVW/ 200 20kUPRightSIFTINRIA2  4000 ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data
	 * SanFran:		yarn jar MapRed_makeGANN.jar SURFVW.MapRed_makeGANN -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DselectPhotosPath=ImageR/BenchMark/SanFrancisco/SanFrancisco_Q-DPCI_transIndex_L_to_S.hashMap -DrandomPhoNum_forLearnNodeLink=100000 -DrandomPhoNum_forTestNodeLink=50000 -DdataLabel_forLearnNodeLink=_SanFranQDPCI@noUse -DdataLabel_forTestNodeLink=_SanFranQDPCI@noUse -DrandFeatNum_forLearnNodeLink=5000 -DrandFeatNum_forTestNodeLink=5000 -DmiddleNodeNum=1000 -DtargetFeature=SIFT-binTool-UPRightINRIA2 -DBinTool_SIFT=ImageR/forVW/SIFT_INRIA2/INRIA2_compute_descriptors_linux64 -DtargetImgSize=786432 -DisIntensityNorm=true ImageR/BenchMark/SanFrancisco/forVW/SIFT-binTool-UPRightINRIA2_SanFranQDPCI_forVW-RFea1000-RPho50k_KMean_VW100k/loop-99/part-r-00000 ImageR/BenchMark/SanFrancisco/forVW/ 200 100kUPRightSIFTINRIA2 4000 ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data
	 * SanFran:		yarn jar MapRed_makeGANN.jar SURFVW.MapRed_makeGANN -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DselectPhotosPath=ImageR/BenchMark/SanFrancisco/SanFrancisco_Q-DPCI_transIndex_L_to_S.hashMap -DrandomPhoNum_forLearnNodeLink=100000 -DrandomPhoNum_forTestNodeLink=50000 -DdataLabel_forLearnNodeLink=_SanFranQDPCI@noUse -DdataLabel_forTestNodeLink=_SanFranQDPCI@noUse -DrandFeatNum_forLearnNodeLink=5000 -DrandFeatNum_forTestNodeLink=5000 -DmiddleNodeNum=1000 -DtargetFeature=SIFT-binTool-UPRightOxford1 -DBinTool_SIFT=ImageR/forVW/SIFT_Oxford1/Oxford_extract_features.ln -DtargetImgSize=786432 -DisIntensityNorm=true ImageR/BenchMark/SanFrancisco/forVW/SIFT-binTool-UPRightOxford1_SanFranQDPCI_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000  ImageR/BenchMark/SanFrancisco/forVW/ 200 20kUPRightSIFTOxford1  4000 ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data
	 * ME15:		yarn jar MapRed_makeGANN.jar SURFVW.MapRed_makeGANN -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DselectPhotosPath=MediaEval15/ME15_photos_L_to_S_train.hashMap -DrandomPhoNum_forLearnNodeLink=100000 -DrandomPhoNum_forTestNodeLink=50000 -DdataLabel_forLearnNodeLink=_ME15D@noUse -DdataLabel_forTestNodeLink=_ME15D@noUse -DrandFeatNum_forLearnNodeLink=5000 -DrandFeatNum_forTestNodeLink=5000 -DmiddleNodeNum=1000 -DtargetFeature=SIFT-binTool-UPRightOxford1 -DBinTool_SIFT=ImageR/forVW/SIFT_Oxford1/Oxford_extract_features.ln -DtargetImgSize=786432 -DisIntensityNorm=false MediaEval15/forVW/SIFT-binTool-UPRightOxford1_ME15D_forVW-RFea1000-RPho50k_KMean_VW65k/loop-99/part-r-00000  MediaEval15/forVW/ 200 65kUPRightSIFTOxford1  4000 Webscope100M/ME14_Crawl/Photos Webscope100M/ME14_Crawl/Photos
	 */
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_makeGANN(), args);
		System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem hdfs=FileSystem.get(conf); 
		String[] otherArgs = args; //use this to parse args!
		int randPhoNum, randFeatNum; String dataLabel_startInd_totNum, dataLabel;
		int middleNodeNum=Integer.valueOf(conf.get("middleNodeNum"));
		boolean isSIFTBinTool=conf.get("targetFeature").startsWith("SIFT-binTool"); //SURF, SIFT-binTool-Oxford2, SIFT-binTool-INRIA2
		
		String vwPath=otherArgs[0];//ImageR/BenchMark/Oxford/forVW/randFeat30000_AllOxford5K_KMean_VW1000k/loop-30/part-r-00000
		String workPath=otherArgs[1];//ImageR/BenchMark/Oxford/forVW/
		int maxInterNum=Integer.valueOf(otherArgs[2]);//200
		
		String vwNumLabel=otherArgs[3];//1000k, 20kUPRightSIFTINRIA2
		
		int job2_2RedNum=Integer.valueOf(otherArgs[4]);//4000
		
		//set imagesPath for learn node-link
		String imagesPath_forLearnNodeLink=otherArgs[5];
		System.out.println("imagesPath_forLearnNodeLink:"+imagesPath_forLearnNodeLink);
		ArrayList<Path> imageSeqPaths_forLearnNodeLink = General_Hadoop.addImgPathsFromMyDataSet(imagesPath_forLearnNodeLink);
		
		//set imagesPath for test node-link
		String imagesPath_forTestNodeLink=otherArgs[6];
		System.out.println("imagesPath_forTestNodeLink:"+imagesPath_forTestNodeLink);
		ArrayList<Path> imageSeqPaths_forTestNodeLink = General_Hadoop.addImgPathsFromMyDataSet(imagesPath_forTestNodeLink);
		
		//************************* Job1: kmean-build middle node ***********************
		String out_Job1=workPath+"MiddleNode"+middleNodeNum+"_onVW"+vwNumLabel+"_maxLoop"+maxInterNum+"/";
		String middleNodePath=MapRed_KMean.runHadoop_Single(conf, new Path[]{new Path(vwPath)}, out_Job1, middleNodeNum, maxInterNum); //input data format: IntWritable.class, FloatArr.class
//		String middleNodePath=out_Job1+"/part-r-00000";	
				
		//************************* Job2: learn_nodeLinks ***********************
		//set selectPhotosPath
		dataLabel_startInd_totNum=conf.get("dataLabel_forLearnNodeLink"); //_MedEval3M@1_3185258
		dataLabel=dataLabel_startInd_totNum.split("@")[0];
		randPhoNum=Integer.valueOf(conf.get("randomPhoNum_forLearnNodeLink"));
		randFeatNum=Integer.valueOf(conf.get("randFeatNum_forLearnNodeLink"));
		conf.set("randomFeatNum",conf.get("randFeatNum_forLearnNodeLink"));
		SelectSamples.setRandomSelectedSample(randPhoNum, "randSelectPhotosPath", "selectPhotosPath", dataLabel_startInd_totNum.split("@")[1], workPath, dataLabel, conf);	
		//set nodeLinksPath
		String taskLabel="D"+dataLabel+"-RFea"+randFeatNum+"-RPho"+randPhoNum/1000+"k";
		conf.set("nodeLink_learned", workPath+taskLabel+"_node_vw_links_M"+middleNodeNum+"_VW"+vwNumLabel+"_I"+maxInterNum+".ArrayList_HashSet");
		String out_Job2_1=workPath+taskLabel+"_MiddleNode"+middleNodeNum+"_onVW"+vwNumLabel+"_raw"+conf.get("targetFeature")+"_forLearnNodeLink";
		String out_Job2_2=workPath+taskLabel+"_MiddleNode"+middleNodeNum+"_onVW"+vwNumLabel+"_learn_node_Links_temp";
		//run
		learn_nodeLinks(isSIFTBinTool, conf, imageSeqPaths_forLearnNodeLink, conf.get("randSelectPhotosPath"), vwPath, middleNodePath, out_Job2_1, out_Job2_2, job2_2RedNum);
		hdfs.delete(new Path(out_Job2_1), true); 
		hdfs.delete(new Path(out_Job2_2), true);
		
		//************************* Job3: test_nodeLinks ***********************
		String out_Job3_1=workPath+taskLabel+"_MiddleNode"+middleNodeNum+"_onVW"+vwNumLabel+"_rawSURF_forTestNodeLink";
		String out_Job3_2=workPath+taskLabel+"_MiddleNode"+middleNodeNum+"_onVW"+vwNumLabel+"_test_node_Links_temp";
		//set selectPhotosPath
		dataLabel_startInd_totNum=conf.get("dataLabel_forTestNodeLink"); //_66M@3185259_77000000
		dataLabel=dataLabel_startInd_totNum.split("@")[0];
		randPhoNum=Integer.valueOf(conf.get("randomPhoNum_forTestNodeLink"));
		conf.set("randomFeatNum",conf.get("randFeatNum_forTestNodeLink"));
		SelectSamples.setRandomSelectedSample(randPhoNum, "randSelectPhotosPath", "selectPhotosPath", dataLabel_startInd_totNum.split("@")[1], workPath, dataLabel, conf);	
		//run
		test_nodeLinks(isSIFTBinTool, conf, imageSeqPaths_forTestNodeLink, conf.get("randSelectPhotosPath"), vwPath, middleNodePath, conf.get("nodeLink_learned"), out_Job3_1, out_Job3_2, job2_2RedNum);
		hdfs.delete(new Path(out_Job3_1), true); 
		hdfs.delete(new Path(out_Job3_2), true);
		
		hdfs.close();
		return 0;
		
		
	}
	
	public static void learn_nodeLinks(boolean isSIFTBinTool, Configuration conf, ArrayList<Path> imageSeqPaths_forLearnNodeLink, String selectPhotosPath,
			String vwPath, String middleNodePath, String out_Job2_1, String out_Job2_2, int job2_2RedNum) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException{
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		//************************* Job2_1: extract raw feat *********************
		SelectSamples selectSamples=new SelectSamples(selectPhotosPath, false);
		//Distributed cache, add VWPath, pMatrixPath, HEThresholdPath, middleNode, nodeLink_learned, selectPhotosPath
		cacheFilePaths.clear();
		selectSamples.addDistriCache_SelectSamples(cacheFilePaths); //SelSamples path with symLink
		if(isSIFTBinTool){
			cacheFilePaths.add(conf.get("BinTool_SIFT")+"#BinTool_SIFT.exe"); //BinTool_SIFT path with symLink
			General_Hadoop.addToCacheListWithOriNameAsSymLink(cacheFilePaths, conf.get("BinTool_libs"), ",", "");//libs without symLink, needs keep original name
		}
		General_Hadoop.Job(conf, imageSeqPaths_forLearnNodeLink.toArray(new Path[0]), out_Job2_1, "forRawFeat", 1200, 8, 2, true,
				MapRed_makeGANN.class, selectSamples.getMapper(), Partitioner_random.class, null,null,Reducer_RawLocalFeature.class,
				IntWritable.class, BufferedImage_jpg.class, IntWritable.class,FloatArr.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		
		//************************* Job2_2: learn link between middleNode and vw *********************
		//Distributed cache, add centers
		cacheFilePaths.clear();
		cacheFilePaths.add(vwPath+"#centers.file"); //centers path with symLink
		cacheFilePaths.add(middleNodePath+"#middleNodes.file"); //middleNode path with symLink
		General_Hadoop.Job(conf, new Path[]{new Path(out_Job2_1)}, out_Job2_2, "learnlinks", job2_2RedNum, 8, 2, true,
				MapRed_makeGANN.class, null, Partitioner_random.class, null,null,Reducer_findNodeLinks.class,
				IntWritable.class, FloatArr.class, IntWritable.class,IntWritable.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		
		//************************* Job2_3: combine links *********************
		General_Hadoop.Job(conf, new Path[]{new Path(out_Job2_2)}, null, "combine", 1, 8, 2, false,
				MapRed_makeGANN.class, null, null, null,null, Reducer_combine_nodeLink.class,
				IntWritable.class, IntWritable.class, IntWritable.class,IntWritable.class,
				SequenceFileInputFormat.class, NullOutputFormat.class, 10*1024*1024*1024L, 0,
				null,null);
	}
	
	public static void test_nodeLinks(boolean isSIFTBinTool, Configuration conf, ArrayList<Path> imageSeqPaths_forTestLink, String selectPhotosPath,
			String vwPath, String middleNodePath, String nodeLinkPath, String out_Job3_1, String out_Job3_2, int job3_2RedNum) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException{
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		//************************* Job3_1: extract raw feat *********************
		SelectSamples selectSamples=new SelectSamples(selectPhotosPath, false);
		//Distributed cache
		cacheFilePaths.clear();
		selectSamples.addDistriCache_SelectSamples(cacheFilePaths); //SelSamples path with symLink
		if(isSIFTBinTool){
			cacheFilePaths.add(conf.get("BinTool_SIFT")+"#BinTool_SIFT.exe"); //BinTool_SIFT path with symLink
			General_Hadoop.addToCacheListWithOriNameAsSymLink(cacheFilePaths, conf.get("BinTool_libs"), ",", "");//libs without symLink, needs keep original name
		}
		General_Hadoop.Job(conf, imageSeqPaths_forTestLink.toArray(new Path[0]), out_Job3_1, "forRawFeat", 1200, 8, 2, true,
				MapRed_makeGANN.class, selectSamples.getMapper(), Partitioner_random.class, null,null,Reducer_RawLocalFeature.class,
				IntWritable.class, BufferedImage_jpg.class, IntWritable.class,FloatArr.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		
		//************************* Job3_2: learn link between middleNode and vw *********************
		//Distributed cache, add centers
		cacheFilePaths.clear();
		cacheFilePaths.add(vwPath+"#centers.file"); //centers path with symLink
		cacheFilePaths.add(middleNodePath+"#middleNodes.file"); //middleNode path with symLink
		cacheFilePaths.add(nodeLinkPath+"#nodeLink_learned.file"); //nodeLink_learned path with symLink
		General_Hadoop.Job(conf, new Path[]{new Path(out_Job3_1)}, out_Job3_2, "testlinks", job3_2RedNum, 8, 2, true,
				MapRed_makeGANN.class, null, Partitioner_random.class, null,null,Reducer_testNodeLinks.class,
				IntWritable.class, FloatArr.class, IntWritable.class,IntArr.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		
		//************************* Job3_3: combine links *********************
		General_Hadoop.Job(conf, new Path[]{new Path(out_Job3_2)}, null, "combine", 1, 8, 2, true,
				MapRed_makeGANN.class, null, null, null,null, Reducer_combine_testRes.class,
				IntWritable.class, IntArr.class, IntWritable.class,IntWritable.class,
				SequenceFileInputFormat.class, NullOutputFormat.class, 10*1024*1024*1024L, 0,
				null,null);
	}
	
	//********** learn_nodeLinks  ***************************
	public static class Reducer_findNodeLinks extends Reducer<IntWritable,FloatArr,IntWritable,IntWritable>{
		// node links learning, 

		private float[][] centers;
		private float[][] middleNodes;
		
		private int procSamples;
		private long startTime;
		private int dispInter;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			//read center into memory
			System.out.println("before read center, memory:"+General.memoryInfo());
			centers=General_Hadoop.readMatrixFromSeq_floatMatrix(conf, General_Hadoop.getLocalPath("centers.file", conf));
			System.out.println("center-number: "+centers.length+", after read centers, memory:"+General.memoryInfo());
			//read middle-nodes
			middleNodes=General_Hadoop.readMatrixFromSeq_floatMatrix(conf, General_Hadoop.getLocalPath("middleNodes.file", conf));
			System.out.println("middleNodes-number: "+middleNodes.length+", after read middleNodes, memory:"+General.memoryInfo());
			// ***** setup finished ***//
			dispInter=2000; 
			startTime=System.currentTimeMillis();
			procSamples=0;
			System.out.println("Reducer_findCenter setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void reduce(IntWritable key, Iterable<FloatArr> values, Context context) throws IOException, InterruptedException {
			//key: notCare
			//value: feat
			//******** key-featInd maybe duplicated, not care the ID ************		
			for(Iterator<FloatArr> it=values.iterator();it.hasNext();){// loop over all samples		
				FloatArr oneSample=it.next();
				//key: notcare, value: featArr
				float[] oneFeat=oneSample.getFloatArr();
				// assign visual word
				int centerIndex=General.assignFeatToCenter(oneFeat, centers);	
				// assign middle node
				int node=General.assignFeatToCenter(oneFeat, middleNodes);
				context.write(new IntWritable(node), new IntWritable(centerIndex)); //node_vw
				
				procSamples++;
				if (procSamples%dispInter==0){ //debug disp info
					System.out.println();
					System.out.println(procSamples+" procSamples finsihed!..."+General.dispTime(System.currentTimeMillis()-startTime, "min"));
				}
			}

		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one Reducer finished! total Samples in this Reducer: "+procSamples+"..."+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			super.setup(context);
	 	}
	
	}
	
	public static class Reducer_combine_nodeLink extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>  {
		
		ArrayList<HashSet<Integer>> node_vw_links;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//Initialize node_vw_links
			int middleNodeNum=Integer.valueOf(conf.get("middleNodeNum"));
			node_vw_links= new ArrayList<HashSet<Integer>>();
			for (int i = 0; i < middleNodeNum; i++) {
				node_vw_links.add(new HashSet<Integer>());
			}
			System.out.println("Initialize node_vw_links done! middleNodeNum:"+middleNodeNum);
		}
		
		//only 1 reducer, combine node links
		@Override
		public void reduce(IntWritable node, Iterable<IntWritable> vws, Context context) throws IOException, InterruptedException {
			int oneNode=node.get();
			//*** built vw-node links  ******//
			for(Iterator<IntWritable> it=vws.iterator();it.hasNext();){
				int vw=it.next().get();
				node_vw_links.get(oneNode).add(vw);
	        }
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** cleanup***//
			Configuration conf = context.getConfiguration();
			FileSystem fs=FileSystem.get(conf);
			//save node_vw links
			General_Hadoop.writeObject_HDFS(fs, conf.get("nodeLink_learned"), node_vw_links);
			int maxLength=0;  int minLength=999999; 
			for(int i=0;i<node_vw_links.size();i++){
				maxLength=Math.max(node_vw_links.get(i).size(),maxLength);
				minLength=Math.min(node_vw_links.get(i).size(),minLength);
			}
			System.out.println("node_vw_links, link number per node, maxLength: "+maxLength+", minLength:"+minLength);
			System.out.println("node_vw_links saved!"+conf.get("nodeLink_learned"));
			super.setup(context);
	 	}

	}

	//********** test_nodeLinks  ***************************
	public static class Reducer_testNodeLinks extends Reducer<IntWritable,FloatArr,IntWritable,IntArr>{
		// node links test, 
		private float[][] centers;
		private float[][] middleNodes;
		private ArrayList<HashSet<Integer>> node_vw_links;
		private ArrayList<float[]> sampleFeats;
		private int sampleNumForTestTime;
		private Random random;
		private int wrongNum;
		private int procSamples;
		private long startTime;
		private int dispInter;
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			//check file in distributted cache
			General.checkDir(new Disp(true, "", null), ".");
			//read center into memory
			System.out.println("before read center, memory:"+General.memoryInfo());
			centers=General_Hadoop.readMatrixFromSeq_floatMatrix(conf, General_Hadoop.getLocalPath("centers.file", conf));
			System.out.println("center-number: "+centers.length+", after read centers, memory:"+General.memoryInfo());
			//read middle-nodes
			middleNodes=General_Hadoop.readMatrixFromSeq_floatMatrix(conf, General_Hadoop.getLocalPath("middleNodes.file", conf));
			System.out.println("middleNodes-number: "+middleNodes.length+", after read middleNodes, memory:"+General.memoryInfo());
			//load node_vw_links
			node_vw_links= (ArrayList<HashSet<Integer>>) General.readObject("nodeLink_learned.file");
			int maxLength=0;  int minLength=999999; 
			for(int i=0;i<node_vw_links.size();i++){
				maxLength=Math.max(node_vw_links.get(i).size(),maxLength);
				minLength=Math.min(node_vw_links.get(i).size(),minLength);
			}
			System.out.println("node_vw_links: link number per node, maxLength: "+maxLength+", minLength:"+minLength);
			// ***** setup finished ***//
			sampleNumForTestTime=1000;
			sampleFeats=new ArrayList<float[]>(sampleNumForTestTime);
			random=new Random();
			dispInter=2000; 
			startTime=System.currentTimeMillis();
			wrongNum=0;
			procSamples=0;
			System.out.println("Reducer_testNodeLinks setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void reduce(IntWritable key, Iterable<FloatArr> values, Context context) throws IOException, InterruptedException {
			//key: notCare
			//value: feat
			//******** key-featInd maybe duplicated, not care the ID ************		
			for(Iterator<FloatArr> it=values.iterator();it.hasNext();){// loop over all samples		
				FloatArr oneSample=it.next();
				//key: notcare, value: featArr
				float[] oneFeat=oneSample.getFloatArr();
				// true assign visual word
				int centerIndex_ori=General.assignFeatToCenter(oneFeat, centers);	
				// GANN assign visual word
				int centerIndex_GANN=General.assignFeatToCenter_fastGANN(oneFeat, centers, middleNodes, node_vw_links);
				
				if (centerIndex_ori!=centerIndex_GANN) {
					wrongNum++;
				}
				
				procSamples++;
				if (procSamples%dispInter==0){ //debug disp info
					System.out.println();
					System.out.println(procSamples+" procSamples finsihed!..."+General.dispTime(System.currentTimeMillis()-startTime, "min"));
				}
				//add sample to test time
				sampleFeats.add(oneFeat);
				if (sampleFeats.size()>sampleNumForTestTime) {
					sampleFeats.remove(random.nextInt(sampleFeats.size()));
					General.Assert(sampleFeats.size()==sampleNumForTestTime, "err in Reducer_testNodeLinks, sampleFeats.size() should ==sampleNumForTestTime:"+sampleNumForTestTime+", but:"+sampleFeats.size());
				}
				
			}

		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one Reducer finished! total Samples in this Reducer: "+procSamples+", wrongNum: "+wrongNum+" ..... "+General.getPercentInfo(wrongNum, procSamples)+"......"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			context.write(new IntWritable(1), new IntArr(new int[]{procSamples,wrongNum})); //procSamples_wrongNum;
			//test time
			startTime=System.currentTimeMillis();
			for (float[] oneFeat:sampleFeats) {
				// true assign visual word
				General.assignFeatToCenter(oneFeat, centers);
			}
			System.out.println("for "+sampleFeats.size()+" feats, computing time for ori-findCenter: "+General.dispTime(System.currentTimeMillis()-startTime, "s"));
			startTime=System.currentTimeMillis();
			for (float[] oneFeat:sampleFeats) {
				// GANN assign visual word
				General.assignFeatToCenter_fastGANN(oneFeat, centers, middleNodes, node_vw_links);
			}
			System.out.println("for "+sampleFeats.size()+" feats, computing time for fastGANN-findCenter: "+General.dispTime(System.currentTimeMillis()-startTime, "s"));

			super.setup(context);
	 	}
	
	}

	public static class Reducer_combine_testRes extends Reducer<IntWritable,IntArr,IntWritable,IntWritable>  {
				
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}
		
		//only 1 reducer, combine results
		@Override
		public void reduce(IntWritable only1, Iterable<IntArr> tot_wrongs, Context context) throws IOException, InterruptedException {
			int tot=0; int wrong=0;
			for(Iterator<IntArr> it=tot_wrongs.iterator();it.hasNext();){
				int[] tot_wrong=it.next().getIntArr();
				tot+=tot_wrong[0];
				wrong+=tot_wrong[1];
	        }
			System.out.println("finished! total Samples: "+tot+", wrongNum: "+wrong+"....."+General.getPercentInfo(wrong, tot));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.setup(context);
	 	}

	}
}
