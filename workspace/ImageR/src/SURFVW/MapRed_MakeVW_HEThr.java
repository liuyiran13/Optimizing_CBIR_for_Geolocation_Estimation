package SURFVW;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.ejml.data.DenseMatrix64F;

import MyAPI.General.General;
import MyAPI.General.General_EJML;
import MyAPI.General.General_Hadoop;
import MyAPI.SystemCommand.RenewKerberos;
import MyCustomedHaoop.KeyClass.Key_VW_DimInd;
import MyCustomedHaoop.MapRedFunction.MapRed_KMean;
import MyCustomedHaoop.Mapper.SelectSamples;
import MyCustomedHaoop.Partitioner.Partitioner_equalAssign;
import MyCustomedHaoop.Partitioner.Partitioner_random;
import MyCustomedHaoop.Reducer.Reducer_RawLocalFeature;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.FloatArr;

public class MapRed_MakeVW_HEThr extends Configured implements Tool{

	/**
	 * job1:  	extract raw surf feat from selected photos
	 * mapper: read photo and select some
	 * reducer: extract raw feat, save into SeqFile(InterWritable, VectorWritable)
	 * @param 		"selectPhotosPath"  "randomFeatNum" "HEBitNum"
	 * 
	 * @throws Exception 
	 * 
	 * SURF:
	 * Oxford:		yarn jar MapRed_MakeVW_HEThr.jar SURFVW.MapRed_MakeVW_HEThr -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DselectPhotosPath=ImageR/BenchMark/Oxford/Oxford_ori5K_SelPhos_L_to_S.hashMap -DrandomFeatNum_forVW=5000 -DrandomFeatNum_forHEThr=5000 -DrandomPhoNum_forVW=10000 -DrandomPhoNum_forHEThr=10000 -DHEBitNum=64 -DpMatrixPath=ImageR/HE_ProjectionMatrix64-64 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=0 700 ImageR/BenchMark/Oxford/OxfordBuilding.seq ImageR/BenchMark/Oxford/forVW/ _Oxford5K noUSE 100000 100 -1 1000 99 2000
	 * Herve:		yarn jar MapRed_MakeVW_HEThr.jar SURFVW.MapRed_MakeVW_HEThr -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DselectPhotosPath=ImageR/BenchMark/Herve/Herve_ori1.5K_SelAllPhos_L_to_S.hashMap -DrandomFeatNum_forVW=5000 -DrandomFeatNum_forHEThr=5000 -DrandomPhoNum_forVW=10000 -DrandomPhoNum_forHEThr=10000 -DHEBitNum=64 -DpMatrixPath=ImageR/HE_ProjectionMatrix64-64 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=0 700 ImageR/BenchMark/Herve/HerverImage.seq ImageR/BenchMark/Herve/forVW/ _Herve1.5K noUSE 100000 100 -1 1000 99 2000
	 * SanFran:		yarn jar MapRed_MakeVW_HEThr.jar SURFVW.MapRed_MakeVW_HEThr -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DselectPhotosPath=ImageR/BenchMark/SanFrancisco/SanFrancisco_Q-DPCI_transIndex_L_to_S.hashMap -DrandomFeatNum_forVW=1000 -DrandomFeatNum_forHEThr=3000 -DrandomPhoNum_forVW=50000 -DrandomPhoNum_forHEThr=100000 -DHEBitNum=64 -DpMatrixPath=ImageR/HE_ProjectionMatrix64-64 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=0 1000 ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data ImageR/BenchMark/SanFrancisco/forVW/ _SanFranQDPCI noUSE 20000 100 -1 2000 99 3000
	 * MedEva3M: 	yarn jar MapRed_MakeVW_HEThr.jar SURFVW.MapRed_MakeVW_HEThr -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DrandomFeatNum_forVW=1000 -DrandomFeatNum_forHEThr=3000 -DrandomPhoNum_forVW=50000 -DrandomPhoNum_forHEThr=100000 -DHEBitNum=64 -DpMatrixPath=ImageR/HE_ProjectionMatrix64-64 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=0 -Dmapreduce.job.priority=LOW 1000 3M_Photos_SeqFiles ImageR/forVW/ _MedEval3M 1_3185258 20000 100 -1 2000 99 5000
	 * MedEva3M: 	yarn jar MapRed_MakeVW_HEThr.jar SURFVW.MapRed_MakeVW_HEThr -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DrandomFeatNum_forVW=1000 -DrandomFeatNum_forHEThr=3000 -DrandomPhoNum_forVW=50000 -DrandomPhoNum_forHEThr=100000 -DHEBitNum=64 -DpMatrixPath=ImageR/HE_ProjectionMatrix64-64 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=0 -Dmapreduce.job.priority=LOW 1000 3M_Photos_SeqFiles ImageR/forVW/ _MedEval3M 1_3185258 100000 100 -1 2000 99 10000
	 * MedEva3M: 	yarn jar MapRed_MakeVW_HEThr.jar SURFVW.MapRed_MakeVW_HEThr -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DrandomFeatNum_forVW=1000 -DrandomFeatNum_forHEThr=3000 -DrandomPhoNum_forVW=50000 -DrandomPhoNum_forHEThr=100000 -DHEBitNum=64 -DpMatrixPath=ImageR/HE_ProjectionMatrix64-64 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=0 -Dmapreduce.job.priority=LOW 1000 3M_Photos_SeqFiles ImageR/forVW/ _MedEval3M 1_3185258 200000 100 -1 4000 99 10000
	 * MedEva3M: 	yarn jar MapRed_MakeVW_HEThr.jar SURFVW.MapRed_MakeVW_HEThr -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar -DrandomFeatNum_forVW=1000 -DrandomFeatNum_forHEThr=3000 -DrandomPhoNum_forVW=50000 -DrandomPhoNum_forHEThr=100000 -DHEBitNum=64 -DpMatrixPath=ImageR/HE_ProjectionMatrix64-64 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=0 -Dmapreduce.job.priority=LOW 1000 3M_Photos_SeqFiles ImageR/forVW/ _MedEval3M 1_3185258 1000000 100 -1 20000 99 10000
	 * 
	 * SIFT:
	 * MedEva3M: 	yarn jar MapRed_MakeVW_HEThr.jar SURFVW.MapRed_MakeVW_HEThr -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar -DrandomFeatNum_forVW=1000 -DrandomFeatNum_forHEThr=3000 -DrandomPhoNum_forVW=50000 -DrandomPhoNum_forHEThr=100000 -DHEBitNum=64 -DpMatrixPath=ImageR/forVW/HE_ProjectionMatrix64-128 -DtargetFeature=SIFT-binTool-Oxford2 -DBinTool_SIFT=ImageR/forVW/SIFT_Oxford2/Oxford2_extract_features_64bit.ln -DtargetImgSize=786432 -DisIntensityNorm=false -Dmapreduce.job.priority=LOW 700 3M_Photos_SeqFiles ImageR/forVW/SIFT_Oxford2/ _MedEval3M@_1_3185258 20000 100 -1 2000 99 5000
	 * MedEva3M: 	yarn jar MapRed_MakeVW_HEThr.jar SURFVW.MapRed_MakeVW_HEThr -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar -DrandomFeatNum_forVW=1000 -DrandomFeatNum_forHEThr=3000 -DrandomPhoNum_forVW=50000 -DrandomPhoNum_forHEThr=100000 -DHEBitNum=64 -DpMatrixPath=ImageR/forVW/HE_ProjectionMatrix64-128 -DtargetFeature=SIFT-binTool-INRIA2 -DBinTool_SIFT=ImageR/forVW/SIFT_INRIA2/INRIA2_compute_descriptors_linux64 -DtargetImgSize=786432 -DisIntensityNorm=false -Dmapreduce.job.priority=LOW 700 3M_Photos_SeqFiles ImageR/forVW/SIFT_INRIA2/ _MedEval3M@_1_3185258 20000 100 -1 2000 99 5000
	 * MedEva3M: 	yarn jar MapRed_MakeVW_HEThr.jar SURFVW.MapRed_MakeVW_HEThr -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar -DrandomFeatNum_forVW=1000 -DrandomFeatNum_forHEThr=3000 -DrandomPhoNum_forVW=50000 -DrandomPhoNum_forHEThr=100000 -DHEBitNum=64 -DpMatrixPath=ImageR/forVW/HE_ProjectionMatrix64-128 -DtargetFeature=SIFT-binTool-VLFeat -DBinTool_SIFT=ImageR/forVW/SIFT_VLFeat/VLFeat09_sift_linux64 -DBinTool_libs=ImageR/forVW/SIFT_VLFeat/libvl.so -DtargetImgSize=786432 -DisIntensityNorm=false -Dmapreduce.job.priority=LOW 700 3M_Photos_SeqFiles ImageR/forVW/SIFT_VLFeat/ _MedEval3M@_1_3185258 20000 100 -1 2000 99 5000
	 * SanFran:		yarn jar MapRed_MakeVW_HEThr.jar SURFVW.MapRed_MakeVW_HEThr -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DselectPhotosPath=ImageR/BenchMark/SanFrancisco/SanFrancisco_Q-DPCI_transIndex_L_to_S.hashMap -DrandomFeatNum_forVW=1000 -DrandomFeatNum_forHEThr=3000 -DrandomPhoNum_forVW=50000 -DrandomPhoNum_forHEThr=100000 -DHEBitNum=128 -DpMatrixPath=ImageR/HE_ProjectionMatrix128-128 -DtargetFeature=SIFT-binTool-UPRightINRIA2 -DBinTool_SIFT=ImageR/forVW/SIFT_INRIA2/INRIA2_compute_descriptors_linux64 -DtargetImgSize=786432 -DisIntensityNorm=true 1000 ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data ImageR/BenchMark/SanFrancisco/forVW/ _SanFranQDPCI noUSE 20000 100 -1 2000 99 3000
	 * SanFran:		yarn jar MapRed_MakeVW_HEThr.jar SURFVW.MapRed_MakeVW_HEThr -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DselectPhotosPath=ImageR/BenchMark/SanFrancisco/SanFrancisco_Q-DPCI_transIndex_L_to_S.hashMap 	-DrandomFeatNum_forVW=1000 -DrandomFeatNum_forHEThr=3000 -DrandomPhoNum_forVW=50000 -DrandomPhoNum_forHEThr=100000 -DHEBitNum=128 -DpMatrixPath=ImageR/HE_ProjectionMatrix128-128 -DtargetFeature=SIFT-binTool-UPRightOxford1 -DBinTool_SIFT=ImageR/forVW/SIFT_Oxford1/Oxford_extract_features.ln -DtargetImgSize=786432 -DisIntensityNorm=true 1000 ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data ImageR/BenchMark/SanFrancisco/forVW/ _SanFranQDPCI noUSE 20000 100 -1 2000 99 3000
	 * ME15:		yarn jar MapRed_MakeVW_HEThr.jar SURFVW.MapRed_MakeVW_HEThr -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,jai_core.jar,jai_codec.jar,metadata-extractor-2.6.4.jar,xmpcore.jar -DselectPhotosPath=MediaEval15/ME15_photos_L_to_S_train.hashMap 								-DrandomFeatNum_forVW=1000 -DrandomFeatNum_forHEThr=3000 -DrandomPhoNum_forVW=50000 -DrandomPhoNum_forHEThr=100000 -DHEBitNum=128 -DpMatrixPath=ImageR/HE_ProjectionMatrix128-128 -DtargetFeature=SIFT-binTool-UPRightOxford1 -DBinTool_SIFT=ImageR/forVW/SIFT_Oxford1/Oxford_extract_features.ln -DtargetImgSize=786432 -DisIntensityNorm=false 1000 Webscope100M/ME14_Crawl/Photos MediaEval15/forVW/ _ME15D noUSE 65536 100 -1 2000 99 3000
	 */
	
	public static void main(String[] args) throws Exception {
		
//		preparData();
		
		runHadoop(args);
	}
	
	public static void preparData() throws Exception {
//		String base_Path="O:/ImageRetrieval/SURFVW/";
		
	}
	
	public static void runHadoop(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_MakeVW_HEThr(), args);
		System.exit(ret);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem hdfs=FileSystem.get(conf); 
		String[] otherArgs = args; //use this to parse args!
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		String taskLabel; int randPhoNum;
		
		boolean isSIFTBinTool=conf.get("targetFeature").startsWith("SIFT-binTool"); //SURF, SIFT-binTool-Oxford2, SIFT-binTool-INRIA2
		
		//set RedNum
		int job1_1RedNum=Integer.valueOf(otherArgs[0]);
		System.out.println("job1_1RedNum:"+job1_1RedNum+", for feat extraction");
		
		//set imagesPath
		String imagesPath=otherArgs[1];
		System.out.println("imagesPath:"+imagesPath);
		ArrayList<Path> imageSeqPaths = General_Hadoop.addImgPathsFromMyDataSet(imagesPath);
		System.out.println("imageSeqPaths:"+imageSeqPaths);
		
		//set workPath
		String workPath=otherArgs[2];
		String dataLabel=otherArgs[3]; //_MedEval3M, _Oxford5K
		String startInd_totNum=otherArgs[4]; //1_3185258
		int vwNum=Integer.valueOf(otherArgs[5]);
		int maxInterNum=Integer.valueOf(otherArgs[6]);
		int startLoop=Integer.valueOf(otherArgs[7]);//-1 for begin from random seed, 0 for random-seed is alreay done and begin from loop-0
		int reducerNum_KmeanVW=Integer.valueOf(otherArgs[8]);
		int targetCenterLoop=Integer.valueOf(otherArgs[9]);
		int reducerNum_projectFeat=Integer.valueOf(otherArgs[10]);
		String HEBitNum=conf.get("HEBitNum");
		String VWlabel="_VW"+vwNum/1000+"k";
		RenewKerberos renewTicket=new RenewKerberos();
		//set multiAssFlag
		conf.set("multiAssFlag", "false"); 
		
		//************************* kmean-build VW ***********************
		//set randSelectPhotosPath
		randPhoNum=Integer.valueOf(conf.get("randomPhoNum_forVW"));
		SelectSamples.setRandomSelectedSample(randPhoNum, "randSelectPhotosPath", "selectPhotosPath", startInd_totNum, workPath, dataLabel, conf);	
		SelectSamples selectSamples=new SelectSamples(conf.get("randSelectPhotosPath"), false);
		taskLabel=conf.get("targetFeature")+dataLabel+"_forVW-RFea"+conf.get("randomFeatNum_forVW")+"-RPho"+randPhoNum/1000+"k";
		//********************Job1_1:extract raw feat *********************
		String out_Job1_1=workPath+taskLabel+"_rawFeat";
		if (!hdfs.exists(new Path(out_Job1_1))) {//no feat before
			conf.set("randomFeatNum",conf.get("randomFeatNum_forVW"));
			cacheFilePaths.clear();
			selectSamples.addDistriCache_SelectSamples(cacheFilePaths); //SelSamples path with symLink
			if(isSIFTBinTool){
				cacheFilePaths.add(conf.get("BinTool_SIFT")+"#BinTool_SIFT.exe"); //BinTool_SIFT path with symLink
				General_Hadoop.addToCacheListWithOriNameAsSymLink(cacheFilePaths, conf.get("BinTool_libs"), ",", "");//libs without symLink, needs keep original name
			}
			General_Hadoop.Job(conf, imageSeqPaths.toArray(new Path[0]), out_Job1_1, "forRawFeat", job1_1RedNum, 8, 2, true,
					MapRed_MakeVW_HEThr.class, selectSamples.getMapper(), Partitioner_random.class, null,null, Reducer_RawLocalFeature.class,
					IntWritable.class, BufferedImage_jpg.class, IntWritable.class,FloatArr.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
					cacheFilePaths.toArray(new String[0]),null);
		}
		//************************* Job1_2: kmean-build VW ***********************
		String out_Job1_2=workPath+taskLabel+"_KMean"+VWlabel+"/";
		MapRed_KMean.runHadoop_Parallel(renewTicket, conf, new Path[]{new Path(out_Job1_1)}, out_Job1_2, vwNum, maxInterNum,startLoop,reducerNum_KmeanVW); //input data format: IntWritable.class, FloatArr.class
		
		//************************* HEThr ************************
		renewTicket.renewTicket(true);
		//set randSelectPhotosPath
		randPhoNum=Integer.valueOf(conf.get("randomPhoNum_forHEThr"));
		SelectSamples.setRandomSelectedSample(randPhoNum, "randSelectPhotosPath", "selectPhotosPath", startInd_totNum, workPath, dataLabel, conf);	
		selectSamples=new SelectSamples(conf.get("randSelectPhotosPath"), false);
		taskLabel+="_forHEThr-RFea"+conf.get("randomFeatNum_forHEThr")+"-RPho"+randPhoNum/1000+"k";
		//********************Job2_1:extract raw feat *********************
		String out_Job2_1=workPath+taskLabel+"_rawFeat";
		conf.set("randomFeatNum",conf.get("randomFeatNum_forHEThr"));
		cacheFilePaths.clear();
		selectSamples.addDistriCache_SelectSamples(cacheFilePaths); //SelSamples path with symLink
		if(isSIFTBinTool){
			cacheFilePaths.add(conf.get("BinTool_SIFT")+"#BinTool_SIFT.exe"); //BinTool_SIFT path with symLink
			General_Hadoop.addToCacheListWithOriNameAsSymLink(cacheFilePaths, conf.get("BinTool_libs"), ",", "");//libs without symLink, needs keep original name
		}
		General_Hadoop.Job(conf, imageSeqPaths.toArray(new Path[0]), out_Job2_1, "forRawFeat", job1_1RedNum, 8, 2, true,
				MapRed_MakeVW_HEThr.class, selectSamples.getMapper(), Partitioner_random.class, null,null,Reducer_RawLocalFeature.class,
				IntWritable.class, BufferedImage_jpg.class, IntWritable.class,FloatArr.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		//********************Job2_2: project feat *********************
		renewTicket.renewTicket(true);
		String out_Job2_2=workPath+taskLabel+"_HEThr"+HEBitNum+"_projFeats";
		String centerPath=out_Job1_2+"loop-"+targetCenterLoop+"/part-r-00000";		
		//Distributed cache, add centers
		cacheFilePaths.clear();
		cacheFilePaths.add(centerPath+"#centers.file"); //centers path with symLink
		cacheFilePaths.add(conf.get("pMatrixPath")+"#pMatrix.file"); //pMatrix path with symLink
		General_Hadoop.Job(conf, new Path[]{new Path(out_Job2_1)}, out_Job2_2, "projectFeats", reducerNum_projectFeat, 8, 2, true,
				MapRed_MakeVW_HEThr.class, null, Partitioner_random.class, null,null,Reducer_findCenter_ProjFeat.class,
				IntWritable.class, FloatArr.class, IntWritable.class,FloatArr.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		//****************** Job2_3: findHEThr ******************
		renewTicket.renewTicket(true);
		conf.set("reducerInter", vwNum/2000+"");
		Partitioner_equalAssign partitioner_equalAssign=new Partitioner_equalAssign(conf,true);
		int job2_3RedNum=partitioner_equalAssign.getReducerNum(vwNum);//1 reducer process reducerInter vws
		System.out.println("total reducers for Job2_3, job2_3RedNum:"+job2_3RedNum+", for 1 reducer: "+conf.get("reducerInter")+" vw!");
		String out_Job2_3=workPath+taskLabel+"_HEThr"+HEBitNum+VWlabel+"_KMloop"+targetCenterLoop+"_temp";
		General_Hadoop.Job(conf, new Path[]{new Path(out_Job2_2)}, out_Job2_3, "findHEThr", job2_3RedNum, 8, 2, true,
				MapRed_MakeVW_HEThr.class, Mapper_spliteByDim.class, partitioner_equalAssign.getPartitioner(), null,null,Reducer_getThreshold.class,
				Key_VW_DimInd.class, FloatWritable.class, IntWritable.class,FloatArr.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
				null,null);
		//****************** Job2_4: orgHEThr ******************
		renewTicket.renewTicket(true);
		String out_Job2_4=workPath+taskLabel+"_HEThr"+HEBitNum+VWlabel+"_KMloop"+targetCenterLoop;
		cacheFilePaths.clear();
		cacheFilePaths.add(centerPath+"#centers.file"); //centers path with symLink
		cacheFilePaths.add(conf.get("pMatrixPath")+"#pMatrix.file"); //pMatrix path with symLink
		General_Hadoop.Job(conf, new Path[]{new Path(out_Job2_3)}, out_Job2_4, "orgHEThr", 1, 8, 2, true,
				MapRed_MakeVW_HEThr.class, null, null, null,null,Reducer_organiseThreshold.class,
				IntWritable.class, FloatArr.class, IntWritable.class,FloatArr.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		
		//clean-up		
		General_Hadoop.deleteIfExist(out_Job1_1, hdfs);
		General_Hadoop.deleteIfExist(out_Job2_1, hdfs);
		General_Hadoop.deleteIfExist(out_Job2_2, hdfs);
		General_Hadoop.deleteIfExist(out_Job2_3, hdfs);
		hdfs.close();
		return 0;

	}

	//******** job3_1 **************	
	public static class Reducer_findCenter_ProjFeat extends Reducer<IntWritable,FloatArr,IntWritable,FloatArr>{
		/**
		 * hammming therthod learning, 
		 * classy SURF visual word, compute project feat,
		 */
		
		private float[][] centers;
		
		private DenseMatrix64F pMatrix;
		
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
			pMatrix=(DenseMatrix64F) General.readObject("pMatrix.file");
			// ***** setup finished ***//
			dispInter=1000; 
			startTime=System.currentTimeMillis();
			procSamples=0;
			System.out.println("Reducer_findCenter setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void reduce(IntWritable key, Iterable<FloatArr> values, Context context) throws IOException, InterruptedException {
			//key: featInd
			//value: feat
			//******** key-featInd maybe duplicated, not care the ID ************		
			for(Iterator<FloatArr> it=values.iterator();it.hasNext();){// loop over all samples		
				FloatArr oneSample=it.next();
				//key: notcare, value: featArr
				float[] oneFeat=oneSample.getFloatArr();
				//find center
				int centerInd=General.assignFeatToCenter(oneFeat, centers);
				// compute project feat
				double[] projectFeat=General_EJML.projectFeat(pMatrix, oneFeat);
				// output <key,value>
				context.write(new IntWritable(centerInd), new FloatArr(General.DouArrToFloatArr(projectFeat)));
				
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

	//******** job3_2 **************	
	public static class Mapper_spliteByDim extends Mapper<IntWritable,FloatArr,Key_VW_DimInd,FloatWritable>{
		
		private int procSamples;
		private int dispInter;
		private long startTime, endTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			//set procSamples
			procSamples=0;
			//set dispInter
			dispInter=5000;
			startTime=System.currentTimeMillis(); //startTime
			System.out.println("mapper setup finsihed!");
	 	}
		
		@Override
		protected void map(IntWritable key, FloatArr value, Context context) throws IOException, InterruptedException {
			//key: vw
			//value: feat
			procSamples++;
			int vw=key.get();// SampleName
			for (int i = 0; i < value.getFloatArr().length; i++) {
				context.write(new Key_VW_DimInd(vw, i), new FloatWritable(value.getFloatArr()[i]));
			}
			//disp
			if((procSamples)%dispInter==0){ 							
				endTime=System.currentTimeMillis(); //end time 
				System.out.println( "finished Samples, "+procSamples+" ......"+ General.dispTime (endTime-startTime, "min"));
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
		    endTime=System.currentTimeMillis(); //end time 
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples+" ....."+ General.dispTime ( endTime-startTime, "min"));
	 	}
	}
	
	public static class Reducer_getThreshold extends Reducer<Key_VW_DimInd,FloatWritable,IntWritable,FloatArr>  {

		/**
		 * hammming therthod learning, 
		 * gather projections, get the median projection vetor for each class
		 */
		
		private int HEBitNum;
		private HashMap<Integer, float[]> vw_median;
		private int procSamples;
		private int reduceNum;
		private long startTime;
		private int dispInter;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			HEBitNum=Integer.valueOf(conf.get("HEBitNum"));
			vw_median=new HashMap<>();
			procSamples=0;
			reduceNum=0;
			startTime=System.currentTimeMillis();
			dispInter=HEBitNum;
			super.setup(context);
	 	}
		
		@Override
		public void reduce(Key_VW_DimInd VW_DimInd, Iterable<FloatWritable> projected_Feats, Context context) throws IOException, InterruptedException {
			//each reduce process one vw's one dimInd
			//VW: 0,1,2,3...
			//projected_Feat: HEBitNum-diminsion,
			
			//******** get projection feats for this vw's DimInd ************	
			ArrayList<Float> thisDimFeat=new ArrayList<>();
			for(Iterator<FloatWritable> it=projected_Feats.iterator();it.hasNext();){// loop over all projected_Feats				
				FloatWritable one=it.next();
				thisDimFeat.add(one.get());
			}
			//find media value of this list
			int medianIndex=thisDimFeat.size()/2;
			Collections.sort(thisDimFeat); //sort in ascending order
			float medianValue=thisDimFeat.get(medianIndex);
			//save to vw_median
			float[] thisVWMedian=vw_median.get(VW_DimInd.vw);
			if (thisVWMedian==null) {
				thisVWMedian=new float[HEBitNum];
				thisVWMedian[VW_DimInd.dimInd]=medianValue;
				vw_median.put(VW_DimInd.vw, thisVWMedian);
			}else {
				thisVWMedian[VW_DimInd.dimInd]=medianValue;
			}
			//updata statistics
			procSamples+=thisDimFeat.size();
			reduceNum++;
			if (reduceNum%dispInter==0){ //debug disp info
				System.out.println();
				System.out.println(reduceNum+" reduce, "+procSamples+" procSamples finsihed! current VW_DimInd:"+VW_DimInd
						+", this vw's current saved median:"+General.floatArrToString(vw_median.get(VW_DimInd.vw), "_", "0.000")+" ... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			for (Entry<Integer, float[]> one : vw_median.entrySet()) {
				context.write(new IntWritable(one.getKey()), new FloatArr(one.getValue()));
			}
			System.out.println("one Reducer finished! total "+reduceNum+" reduce(VWVW_DimInd), "+procSamples+" procSamples, in total "+vw_median.size()+" vws ..."+General.dispTime(System.currentTimeMillis()-startTime, "min"));		
			super.setup(context);
	 	}
	}

	public static class Reducer_organiseThreshold extends Reducer<IntWritable,FloatArr,IntWritable,FloatArr>  {

		/**
		 *  organise the median projection vetor for each vw, find no-projectFeat vw, make one for it
		 */
		
		private float[][] centers;
		private boolean[] vwHasFeat;
		private int reduceNum;
		private long startTime;
		private int dispInter;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			//read center into memory
			System.out.println("before read center, memory:"+General.memoryInfo());
			centers=General_Hadoop.readMatrixFromSeq_floatMatrix(conf, General_Hadoop.getLocalPath("centers.file", conf));
			System.out.println("center-number: "+centers.length+", after read centers, memory:"+General.memoryInfo());
			vwHasFeat=new boolean[centers.length]; //save whethe one vw has feat assigned to it
			reduceNum=0;
			startTime=System.currentTimeMillis();
			dispInter=1000;
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable VW, Iterable<FloatArr> Medians, Context context) throws IOException, InterruptedException {
			//VW: 0,1,2,3...
			//Medians: HEBitNum-diminsion
			FloatArr median=General_Hadoop.readOnlyOneElement(Medians, VW+"");
			vwHasFeat[VW.get()]=true;
			context.write(VW, median);
			//updata statistics
			if (reduceNum%dispInter==0){ //debug disp info
				System.out.println();
				System.out.println(reduceNum+"-th reduce finsihed! current vw:"+VW+", its median:"+General.floatArrToString(median.getFloatArr(), "_", "0.000")+" ... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			}
			reduceNum++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			int vwNum_noFeatAssign=centers.length-reduceNum;
			System.out.println("one Reducer finished! total "+reduceNum+" reduce(VW), vwNum_noFeatAssign:"+vwNum_noFeatAssign+"..."+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			if (vwNum_noFeatAssign!=0) {//some vws do not have feat assigned
				DenseMatrix64F pMatrix=(DenseMatrix64F) General.readObject("pMatrix.file");
				for (int i = 0; i < vwHasFeat.length; i++) {
					if (!vwHasFeat[i]) {//this vw do not have feat assigned, then project its center to PMatrix and use the projection as threshold
						DenseMatrix64F projectFeat=General_EJML.matrix_Mut(pMatrix, General_EJML.floatArrToDenseMatrix(centers[i], false)); //1-coloumn vector	
						context.write(new IntWritable(i), new FloatArr(General.DouArrToFloatArr(projectFeat.data)));
						System.out.println("vw: "+i+" do not have feat assigned, then project its center to PMatrix and use the projection as threshold");
					}
				}
			}			
			super.setup(context);
	 	}	
	}

}
