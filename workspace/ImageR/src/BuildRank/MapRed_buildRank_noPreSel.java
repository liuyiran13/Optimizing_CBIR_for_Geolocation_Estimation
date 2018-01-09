//package BuildRank;
//
//import java.io.BufferedReader;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.TreeSet;
//import java.util.Map.Entry;
//import java.util.Random;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.MapFile;
//import org.apache.hadoop.io.SequenceFile;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.ejml.data.DenseMatrix64F;
//
//import boofcv.struct.image.ImageFloat32;
//import MyAPI.General.General;
//import MyAPI.General.General_BoofCV;
//import MyAPI.General.General_Hadoop;
//import MyAPI.General.ComparableCls.slave_masterFloat_DES;
//import MyCustomedHaoop.Combiner.Combiner_combine_IntArr_HESig_ShortArr_Arr;
//import MyCustomedHaoop.KeyClass.Key_QID_DID;
//import MyCustomedHaoop.KeyClass.Key_QID_VW;
//import MyCustomedHaoop.Mapper.selectSamples.Mapper_selectSamples_hashMap;
//import MyCustomedHaoop.Partitioner.Partitioner_forSearchTVector;
//import MyCustomedHaoop.Partitioner.Partitioner_random;
//import MyCustomedHaoop.Partitioner.Partitioner_random_sameKey;
//import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
//import MyCustomedHaoop.ValueClass.DocAllMatchFeats;
//import MyCustomedHaoop.ValueClass.DocInfo;
//import MyCustomedHaoop.ValueClass.DouArr_ShortArr;
//import MyCustomedHaoop.ValueClass.DouArr_ShortArr_SURFpoint_ShortArr;
//import MyCustomedHaoop.ValueClass.HESig;
//import MyCustomedHaoop.ValueClass.IntArr_HESig_ShortArr_Arr;
//import MyCustomedHaoop.ValueClass.IntList_FloatList;
//import MyCustomedHaoop.ValueClass.MatchFeat_Arr;
//import MyCustomedHaoop.ValueClass.SURFfeat;
//import MyCustomedHaoop.ValueClass.SURFpoint;
//import MyCustomedHaoop.ValueClass.SURFpoint_ShortArr;
//
//public class MapRed_buildRank_noPreSel extends Configured implements Tool{
//
//	/**
//	 * 
//	 * job1_1: extract query's SURF raw feats
//	 * mapper:  selected query, 
//	 * reducer: extract feat, output double[][], SURFpoint[]
//	 * @param: "mapred.SelQuerys"
//	 * 
//	 * job1_2: read query's SURF raw feats, save query_SURFpoint into MapFile
//	 * 
//	 * job1_3: read query's SURF raw feats, make HESig
//	 * @param: "mapred.VWPath"  "mapred.pMatrixPath"  "mapred.HEThresholdPath" "mapred.middleNode" "mapred.nodeLink_learned"
//	 *
//	 * job2: Search TVector, get query_MatchFeat for each vw
//	 * @param: "mapred.TVectorPath"  "mapred.HMDistThr" "mapred.VW_PartitionerIDs_Path"
//	 * 
//	 * job3: combine query_MatchFeat from each vw, build final rank for query 
//	 * @param: "mapred.topRank" "mapred.docInfoPath" "mapred.HPM_level" "mapred.HSigBit" "mapred.HMDistThr"
//	 * 
//	 * job4: save all querys' rank into one MapFile
//	 * 
//	 * 
//	 * 
//	 * @throws Exception 
//	 * @command_example: 
//	 * 
//	 * 3M:		hadoop jar MapRed_buildRank_noPreSel.jar BuildRank.MapRed_buildRank_noPreSel -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90 -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix -Dmapred.HEThresholdPath=ImageR/HE_Thresholds -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.ArrayList_HashSet -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=ICMR2013/Querys_100K_LtoS_from_D3M_ICMR2013.hashMap -Dmapred.TVectorPath=ImageR/TVector_3M_MapFile -Dmapred.HMDistThr=12 -Dmapred.docInfoPath=ImageR/photoFeatNum_3M -Dmapred.VW_PartitionerIDs_Path=ImageR/VW_PartitionerIDs_forBuildRank3M_100kQ_mutAssign.intArr -Dmapred.topRank=10000 1000 1000 1 3M_Photos_SeqFiles ImageR/SearchResult_D3M_Q100K_ICMR13 
//	 * MEva13:	hadoop jar MapRed_buildRank_noPreSel.jar BuildRank.MapRed_buildRank_noPreSel -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90 -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix -Dmapred.HEThresholdPath=ImageR/HE_Thresholds -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.ArrayList_HashSet -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=MediaEval13/Querys/ -Dmapred.TVectorPath=MediaEval13/TVector_MEva13_9M_MapFile -Dmapred.HMDistThr=12 -Dmapred.docInfoPath=MediaEval13/photoFeatNum_MEva13_9M -Dmapred.VW_PartitionerIDs_Path=MediaEval13/VW_PartitionerIDs_forBuildRank9M30KQ_mutAssign.intArr -Dmapred.topRank=10000 1000 1000 66M_Phos_Seqs/ MediaEval13/ranks/SURF_D9M_Q250K 
//	 * Herve:	hadoop jar MapRed_buildRank_noPreSel.jar BuildRank.MapRed_buildRank_noPreSel -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.job.priority=LOW -Dmapred.VWPath=ImageR/SURFVW_20K_I90 -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix -Dmapred.HEThresholdPath=ImageR/HE_Thresholds -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.ArrayList_HashSet -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/HerveImage/HerverImage_querys_transIndex_L_to_S.hashMap -Dmapred.TVectorPath=ImageR/BenchMark/HerveImage/TVector_1M_Herve_MapFile -Dmapred.HMDistThr=12 -Dmapred.docInfoPath=ImageR/BenchMark/HerveImage/photoFeatNum_1M_Herve -Dmapred.VW_PartitionerIDs_Path=ImageR/BenchMark/HerveImage/VW_PartitionerIDs_forBuildRank1M_500Q.intArr -Dmapred.topRank=10000000 100 100 1 ImageR/BenchMark/HerveImage/HerverImage_query.seq ImageR/BenchMark/HerveImage/SearchResult_Herve_1M_HD12_VW20K 
//	 * TMM_GVR: hadoop jar MapRed_buildRank_noPreSel.jar BuildRank.MapRed_buildRank_noPreSel -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90 -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix -Dmapred.HEThresholdPath=ImageR/HE_Thresholds -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.ArrayList_HashSet -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=MediaEval13/Querys/ -Dmapred.TVectorPath=TMM_GVR/imagR/TVector_MEva13_9M -Dmapred.HMDistThr=12 -Dmapred.docInfoPath=TMM_GVR/imagR/docInfo_MEva13_9M -Dmapred.TVectorInfoPath=TMM_GVR/imagR/TVectorInfo_MEva13_9M -Dmapred.VW_PartitionerIDs_Path=MediaEval13/VW_PartitionerIDs_forBuildRank9M30KQ_mutAssign.intArr -Dmapred.HPM_level=6 -Dmapred.reRankByHPM=1000 -Dmapred.topRank=10000 1000 1000 66M_Phos_Seqs/ TMM_GVR/imagR/ranks/SURF_D9M_Q250K 
//	 */
//	
//	public static void main(String[] args) throws Exception {
////		timeTest();
//		
////		prepareData();
//		
//		runHadoop(args);
//	}
//	
//	public static void runHadoop(String[] args) throws Exception {
//		int ret = ToolRunner.run(new MapRed_buildRank_noPreSel(), args);
//		System.exit(ret);
//	}
//	
//	@SuppressWarnings("unchecked")
//	public static void prepareData() throws Exception {
////		//***** for 3M random sellected querys *********//
////		int totNum=3185258; int queryNum=100*1000;
////		int[] randInds=General.randIndex(totNum);
////		HashMap<Integer, Integer> query_transIndex_LtoS= new HashMap<Integer, Integer>(queryNum);
////		for (int i = 0; i < queryNum; i++) {
////			query_transIndex_LtoS.put(randInds[i], randInds[i]);
////		}
////		General.writeObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/Querys_100K_LtoS_from_D3M.hashMap", query_transIndex_LtoS);
//		
////		//***** for ICMR2013 3M random sellected querys *********//
////		String quryFilePath="D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/QDP/3M/";
////		String[] fileNames={"topLocGVSize_ori.oriRight","topLocGVSize_ori.oriWrong"};
////		int queryNum=100*1000; String line1;
////		HashMap<Integer, Integer> query_transIndex_LtoS= new HashMap<Integer, Integer>(queryNum);
////		for (int i = 0; i < fileNames.length; i++) {
////			BufferedReader inputStreamFeat = new BufferedReader(new InputStreamReader(new FileInputStream(quryFilePath+fileNames[i]), "UTF-8"));
////			while((line1=inputStreamFeat.readLine())!=null){//Q259_G4_R1:	0	0	0	0	0	0	0	0	0	0	
////				int queryName=Integer.valueOf(line1.split(":")[0].split("_")[0].substring(1));
////				query_transIndex_LtoS.put(queryName, queryName);
////			}
////			inputStreamFeat.close();
////		}
////		System.out.println("total query num:"+query_transIndex_LtoS.size());
////		General.writeObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/Querys_100K_LtoS_from_D3M_ICMR2013.hashMap", query_transIndex_LtoS);
//		
//		//***** check photoFeatNum *********//
////		int[] photoFeatNum=(int[]) General.readObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ImageRetrieval/photoFeatNum_3M");
////		int[] photoFeatNum=(int[]) General.readObject("O:/MediaEval13/photoFeatNum_MEva13_9M");
////		int noFeatPhoNum=0;
////		for (int pho_i : photoFeatNum) {
////			if (pho_i==0) {
////				noFeatPhoNum++;
////			}
////		}
////		System.out.println("totPhoNum:"+photoFeatNum.length+", some photo no feat, noFeatPhoNum:"+noFeatPhoNum);
//		
//		//***** for making sub-query-set for 9M dataset, as when build rank, doc_scores take a lot space if dataset is large, so decrease the query number ************
//		int querySetSize=30*1000;
//		HashMap<Integer, Integer> totQ=(HashMap<Integer, Integer>) General.readObject("O:/MediaEval13/MEval13_L_to_S_test.hashMap");
//		Random rand=new Random();
//		ArrayList<HashMap<Integer, Integer>> Qsets =General.randSplitHashMap(rand, totQ, 0, querySetSize);
//		int totQnum=0;
//		for (int i = 0; i < Qsets.size(); i++) {
//			General.writeObject("O:/MediaEval13/Querys/Q"+i, Qsets.get(i));
//			System.out.println(i+", "+Qsets.get(i).size());
//			totQnum+=Qsets.get(i).size();
//		}
//		General.Assert(totQnum==totQ.size(), "err, totQnum:"+totQnum+", should =="+totQ.size());
//		System.out.println("taget querySetSize:"+querySetSize+", totQnum:"+totQnum+", should =="+totQ.size());
//		
//	}
//	
//	public static void timeTest() throws Exception {
//		
////		//******** time test ***********/
////		int TVectorLength=10*1000*1000; int HMDistThr=12; int matchNum=0; int HElength=64;
////		//make HESig_Bytes
////		BitSet HESig=new BitSet(HElength);
////		for(int i=0;i<HElength;i++)
////			HESig.set(i);// set i-th == true 
////		System.out.println(HESig);
////		byte[] HESig_Bytes=General.BitSettoByteArray(HESig); 
////		//make HESig_Bytes_q
////		BitSet HESig_q=new BitSet(HElength);
////		for(int i=0;i<HElength;i+=3)
////			HESig_q.set(i);// set i-th == true 
////		System.out.println(HESig_q);
////		byte[] HESig_Bytes_q=General.BitSettoByteArray(HESig_q);
////		//test HammingDist Time
////		long startTime=System.currentTimeMillis(); //startTime
////		int hammingDist=0;
////		for(int j=0;j<TVectorLength;j++){
////			hammingDist=General.get_DiffBitNum(HESig_Bytes_q, HESig_Bytes);// computing time: 15% of BigInteger!!
//////			hammingDist=(new BigInteger(HESig_Bytes_q)).xor(new BigInteger(HESig_Bytes)).bitCount(); //slow!!
////			if(hammingDist<=HMDistThr){
////				matchNum++;
////			}
////		}
////		long endTime=System.currentTimeMillis(); //endTime
////		System.out.println("hammingDist:"+hammingDist+", make HammingDist times:"+TVectorLength+"...."+General.dispTime(endTime-startTime, "ms"));
//
//	}
//
//	@Override
//	public int run(String[] args) throws Exception {
//		Configuration conf = getConf();
//		FileSystem fs=FileSystem.get(conf);
//		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); //use this to parse args!
//		String homePath="hdfs://p-head03.alley.sara.nl/user/yliu/";
//		ArrayList<String> cacheFilePaths=new ArrayList<String>();
//		
//		//set job2RedNum based on VW_PartitionerIDs for job2
//		Path VW_PartitionerIDs_Path = new Path(homePath+conf.get("mapred.VW_PartitionerIDs_Path"));
//		int job2RedNum=0; //reducer number for seachTVector
//		try {
//			int[] PaIDs=(int[]) General_Hadoop.readObject_HDFS(fs, VW_PartitionerIDs_Path.toString());
//			job2RedNum=PaIDs[PaIDs.length-1]+1; //PaIDs: values from 0!
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		}
//		//set HSigBit
//		BufferedReader intstr_data = new BufferedReader(new InputStreamReader(fs.open(new Path(homePath+conf.get("mapred.HEThresholdPath")))));
//		int HSigBit=intstr_data.readLine().split(",").length;
//		intstr_data.close();
//		conf.set("mapred.HSigBit", HSigBit+"");
//		
//		//set reducer number
//		int job1_1RedNum=Integer.valueOf(otherArgs[0]); //reducer number
//		System.out.println("job1_1RedNum:"+job1_1RedNum);
//		int job3RedNum=Integer.valueOf(otherArgs[1]); //reducer number
//		System.out.println("job3RedNum:"+job3RedNum);
//		//set input/output path
//		String In=otherArgs[2]; //input path
//		System.out.println("In:"+In);
//		String out=otherArgs[3]; //output path
//		System.out.println("out:"+out);
//		//set rankLabel
//		String rankLabel="_HD"+conf.get("mapred.HMDistThr")+"-"+HSigBit+"_HPM"+conf.get("mapred.HPM_level")+"-"+conf.get("mapred.reRankByHPM")+"_top"+Integer.valueOf(conf.get("mapred.topRank"))/1000+"K";
//		System.out.println("rankLabel:"+rankLabel);
//		//set selected querys set
//		ArrayList<String> selQuerys=new ArrayList<String>(); 
//		String queryHashMapPath=homePath+conf.get("mapred.SelQuerys");
//		if (fs.isFile(new Path(queryHashMapPath))) {
//			selQuerys.add(queryHashMapPath);
//		}else {
//			FileStatus[] files= fs.listStatus(new Path(queryHashMapPath));
//			for (int i = 0; i < files.length; i++) {
//				selQuerys.add(files[i].getPath().toString());
//			}
//		}
//		System.out.println("selQuerys:\n"+selQuerys+"\n");
//		
//		//******* add input images  ***************
//		ArrayList<Path> imageSeqPaths = new ArrayList<Path>();
//		//a.set image sequence file paths: 3M, missiing blocks.... In=otherArgs[0]: 3M_Photos_SeqFiles
////		imageSeqPaths.add(new Path(In));
//		//b.set image sequence file paths, 66M, In=otherArgs[0]: 66M_Phos_Seqs/
//		double Sym1M=1000*1000;
//		int saveInterval=1000*1000; 
//		int start_loop=3; //should start from 3
//		int end_loop=66;  //66
//		for(int loop_i=start_loop;loop_i<=end_loop;loop_i++){//one loop, one MapFile
//			//set photo range for one file
//			int[] photoRang=new int[2];
//			if(loop_i==3){
//				photoRang[0]=3185259;
//			}else{
//				photoRang[0]=loop_i*saveInterval;
//			}
//			photoRang[1]=(loop_i+1)*saveInterval-1;
//			imageSeqPaths.add(new Path(In+photoRang[0]/Sym1M+"_"+photoRang[1]/Sym1M+"_seq"));
//		}
//		imageSeqPaths.add(new Path(In+"3_66_patch_seq"));
//		imageSeqPaths.add(new Path(In+"missingBlocks_patch_seq"));
//				
//		//**********************    build rank  ************//
////		ArrayList<Path> rankPaths=new ArrayList<Path>();
////		for (int i = 0; i < selQuerys.size(); i++) {
//			int i = 0;
//			String loopLabel="_Q"+i+"_"+(selQuerys.size()-1);		
//			
//			//******* job1_1: extract query's SURF raw feats ******
//			String Out_job1_1=out+rankLabel+"_querySURFRaw"+loopLabel;
////			//Distributed cache, add selectPhotosPath
////			cacheFilePaths.clear();
////			cacheFilePaths.add(selQuerys.get(i)+"#SelSamples.file"); //SelSamples path with symLink
////			General_Hadoop.Job(conf, imageSeqPaths.toArray(new Path[0]), Out_job1_1, "Job1_1_getRawFeats"+loopLabel, job1_1RedNum, 8, 2,
////					MapRed_buildRank_noPreSel.class, Mapper_selectSamples_hashMap.class, Partitioner_random.class,null,Reducer_ExtractSURF.class,
////					IntWritable.class, BufferedImage_jpg.class, IntWritable.class,DouArr_ShortArr_SURFpoint_ShortArr.class,
////					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
////					cacheFilePaths.toArray(new String[0]),null);
//			//******* job1_2: read query's SURF raw feats, save query_SURFpoint into MapFile ******
//			String Out_job1_2=out+rankLabel+"_querySURFpoint"+loopLabel;
////			General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_1)}, Out_job1_2, "Job1_2_getSURFpoint"+loopLabel, 1, 8, 2,
////					MapRed_buildRank_noPreSel.class, null, null,null,Reducer_SaveQueryPoints.class,
////					IntWritable.class, DouArr_ShortArr_SURFpoint_ShortArr.class, IntWritable.class,SURFpoint_ShortArr.class,
////					SequenceFileInputFormat.class, MapFileOutputFormat.class, 10*1024*1024*1024L, 0,
////					null,null);
//			//******* job1_3: read query's SURF raw feats, make HESig ******
//			String Out_job1_3=out+rankLabel+"_queryHESig"+loopLabel;
////			//Distributed cache, add VWPath, pMatrixPath, HEThresholdPath, middleNode, nodeLink_learned
////			cacheFilePaths.clear();
////			cacheFilePaths.add(homePath+conf.get("mapred.VWPath")+"#VWs.file"); //VWs path with symLink
////			cacheFilePaths.add(homePath+conf.get("mapred.pMatrixPath")+"#pMatrix.file"); //VWs path with symLink
////			cacheFilePaths.add(homePath+conf.get("mapred.HEThresholdPath")+"#HEThreshold.file"); //VWs path with symLink
////			cacheFilePaths.add(homePath+conf.get("mapred.middleNode")+"#middleNode.file"); //VWs path with symLink
////			cacheFilePaths.add(homePath+conf.get("mapred.nodeLink_learned")+"#nodeLink_learned.file"); //VWs path with symLink
////			General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_1)}, Out_job1_3, "Job1_3_getHESig"+loopLabel, job1_1RedNum, 8, 2,
////					MapRed_buildRank_noPreSel.class, null, Partitioner_random.class,null,Reducer_MakeHESig.class,
////					IntWritable.class, DouArr_ShortArr_SURFpoint_ShortArr.class, IntWritable.class,IntArr_HESig_ShortArr_Arr.class,
////					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
////					cacheFilePaths.toArray(new String[0]),null);
//			//******* job2: Search TVector, get query_MatchFeat for each vw ******
//			String Out_job2=out+rankLabel+"_MatchFeat"+loopLabel;
////			//add Distributed cache for job2
////			cacheFilePaths.clear();
////			cacheFilePaths.add(homePath+conf.get("mapred.VW_PartitionerIDs_Path")+"#PaIDs.file"); //PaIDs with symLink
////			General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_3)}, Out_job2, "Job2_getMatchFeat"+loopLabel, job2RedNum, 8, 2,
////					MapRed_buildRank_noPreSel.class, null, Partitioner_forSearchTVector.class, Combiner_combine_IntArr_HESig_ShortArr_Arr.class, Reducer_SearchTVector.class,
////					IntWritable.class, IntArr_HESig_ShortArr_Arr.class, Key_QID_VW.class,Int_MatchFeat_ShortArr.class,
////					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
////					cacheFilePaths.toArray(new String[0]),null);
//			//******* job3: combine query_MatchFeat from each vw, build final rank for query ******
//			String Out_job3=out+rankLabel+"_queryRank"+loopLabel;
//			//add Distributed cache for job2
//			cacheFilePaths.clear();
//			cacheFilePaths.add(homePath+Out_job1_2+"/part-r-00000/data#data"); //FilterMapFile data path with symLink
//			cacheFilePaths.add(homePath+Out_job1_2+"/part-r-00000/index#index"); //FilterMapFile data path with symLink
//			cacheFilePaths.add(homePath+conf.get("mapred.docInfoPath")+"/part-r-00000#docInfo.file"); //docInfo with symLink
//			cacheFilePaths.add(homePath+conf.get("mapred.TVectorInfoPath")+"#TVectorInfo.file"); //TVectorInfo with symLink
//			General_Hadoop.Job(conf, new Path[]{new Path(Out_job2)}, Out_job3, "buildRank"+loopLabel, job3RedNum, 8, 2,
//					MapRed_buildRank_noPreSel.class, Mapper_transfer.class, Partitioner_random_sameKey.class,null,Reducer_buildRank.class,
//					Key_QID_DID.class, Int_MatchFeat_Arr.class, IntWritable.class,IntList_FloatList.class,
//					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
//					cacheFilePaths.toArray(new String[0]),null);	
//			
////			//clean-up
////			fs.delete(new Path(Out_job1_1), true);
////			fs.delete(new Path(Out_job1_2), true);
////			fs.delete(new Path(Out_job1_3), true);
////			fs.delete(new Path(Out_job2), true);
////			rankPaths.add(new Path(Out_job3));
////		}
////		//******* job4: save all querys' rank into one MapFile ******
////		General_Hadoop.Job(conf, rankPaths.toArray(new Path[0]), out+rankLabel, "combine&save", 1, 8, 2,
////				MapRed_buildRank_noPreSel.class, null, null, null, Reducer_InOut_1key_1value.class,
////				IntWritable.class, IntList_FloatList.class, IntWritable.class,IntList_FloatList.class,
////				SequenceFileInputFormat.class, MapFileOutputFormat.class, 0, 10,
////				null,null);	
////		//clean-up
////		for (Path path : rankPaths) {
////			fs.delete(path, true);
////		}
//
//		fs.close();
//		return 0;
//	}
//	
//	//******** job1_1 **************	
//	public static class Reducer_ExtractSURF extends Reducer<IntWritable,BufferedImage_jpg,IntWritable,DouArr_ShortArr_SURFpoint_ShortArr>{
//		//Reducer_extractSURF: extract double[][] feats, and SURFfeat_noSig[]
//		private boolean disp;
//		private int procPhotos;
//		private int dispInter;
//		private long startTime, endTime;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			//set procPhotos
//			procPhotos=0;
//			//set dispInter
//			dispInter=50;
//			startTime=System.currentTimeMillis(); //startTime
//			
//			System.out.println("reducer setup finsihed!");
//			disp=true; 
//			super.setup(context);
//	 	}
//		
//		protected void reduce(IntWritable key, Iterable<BufferedImage_jpg> value, Context context) throws IOException, InterruptedException {
//			//key: photoName
//			//value: file content
//			//******** only one in value! ************	
//			int loopNum=0; BufferedImage_jpg photo=new BufferedImage_jpg();
//			for(Iterator<BufferedImage_jpg> it=value.iterator();it.hasNext();){// loop over all BufferedImage_jpg				
//				photo=it.next();
//				loopNum++;
//			}
//			General.Assert(loopNum==1, "error in Reducer_SURFVW_HE_selectQurey! one photoName, one photo, loopNum should == 1, here loopNum="+loopNum);
//					
//			int photoName=key.get();// photoName
//
//			ImageFloat32 img=General_BoofCV.BoofCV_loadImage(photo.getBufferedImage(),ImageFloat32.class); //image content
//			//***classify visual feat to visual word***//
//			double[][] photoFeat=null; ArrayList<SURFpoint> interestPoints=new ArrayList<SURFpoint>();
//			try {
//				photoFeat=General_BoofCV.computeSURF_boofCV_09(img,"2,1,5,true","2000,1,9,4,4", interestPoints);
//			} catch (Exception e) {
//				System.err.println("error when calculating SURF feature for img:"+photoName);
//				System.err.println("error message:"+e.getMessage());
//				throw new InterruptedException("error in Reducer_SURFVW_HE_selectQurey, when calculating SURF feature for img:"+photoName);
//			}
//			
//			if(photoFeat!=null){ // photo has feat(some photos are too small, do not have interest point)
//				//mapper outPut
//				context.write(key, new DouArr_ShortArr_SURFpoint_ShortArr(new DouArr_ShortArr(photoFeat), new SURFpoint_ShortArr(interestPoints)));
//				//debug disp info
//		        if (disp==true){ 
//					System.out.println("\t show one example: ");
//					System.out.println("\t mapout_Key, photoName: "+photoName);
//					System.out.println("\t mapout_Value, photoFeat: number, "+photoFeat.length);
//					disp=false;
//					System.out.println("\t disp:"+disp);
//				}
//			}else{
//				System.err.println("image exist, but no feat for photo: "+photoName);
//				return;
//			}
//			//disp
//			procPhotos++;
//    		if((procPhotos)%dispInter==0){ 							
//				endTime=System.currentTimeMillis(); //end time 
//				System.out.println( "extractSURF photo feat, "+procPhotos+" photos finished!! ......"+ General.dispTime (endTime-startTime, "min"));
//			}
//		}
//
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("\n one reducer finished! total processed photos in this Reducer: "+procPhotos+" ....."+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
//			super.setup(context);
//	 	}
//	}
//
//	//******** job1_2 **************	
//	public static class Reducer_SaveQueryPoints extends Reducer<IntWritable,DouArr_ShortArr_SURFpoint_ShortArr,IntWritable,SURFpoint_ShortArr>{
//		private boolean disp;
//		private int procPhotos;
//		private int dispInter;
//		private long startTime, endTime;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			//set procPhotos
//			procPhotos=0;
//			//set dispInter
//			dispInter=10*1000;
//			startTime=System.currentTimeMillis(); //startTime
//			
//			System.out.println("reducer setup finsihed!");
//			disp=true; 
//			super.setup(context);
//	 	}
//		
//		protected void reduce(IntWritable key, Iterable<DouArr_ShortArr_SURFpoint_ShortArr> value, Context context) throws IOException, InterruptedException {
//			//key: photoName
//			//value: file content
//			//******** only one in value! ************	
//			int loopNum=0; DouArr_ShortArr_SURFpoint_ShortArr photoFeat_IOpoint=new DouArr_ShortArr_SURFpoint_ShortArr();
//			for(Iterator<DouArr_ShortArr_SURFpoint_ShortArr> it=value.iterator();it.hasNext();){// loop over all BufferedImage_jpg				
//				photoFeat_IOpoint=it.next();
//				loopNum++;
//			}
//			General.Assert(loopNum==1, "error in Reducer_SURFVW_HE_selectQurey! one photoName, one photo, loopNum should == 1, here loopNum="+loopNum);
//
//			//mapper outPut
//			IntWritable mapout_Key=key;//photoName
//			SURFpoint_ShortArr mapout_Value=photoFeat_IOpoint.points; //SURFpoints
//			context.write(mapout_Key, mapout_Value);
//			
//			//debug disp info
//	        if (disp==true){ 
//	        	System.out.println("\t show one example: ");
//				System.out.println("\t mapout_Key, photoName: "+mapout_Key.toString());
//				System.out.println("\t mapout_Value, SURFpoints: number_"+mapout_Value.getArr().length);
//				disp=false;
//				System.out.println("\t disp:"+disp);
//			}
//			//disp
//	        procPhotos++;
//    		if((procPhotos)%dispInter==0){ 							
//				endTime=System.currentTimeMillis(); //end time 
//				System.out.println( "Save Query photo Points, "+procPhotos+" photos finished!! ......"+ General.dispTime (endTime-startTime, "min"));
//			}
//		}
//		
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//		    endTime=System.currentTimeMillis(); //end time 
//			System.out.println("\n one reducer finished! total processed photos in this Reducer: "+procPhotos+" ....."+ General.dispTime ( endTime-startTime, "min"));
//			super.setup(context);
//	 	}
//	}
//		
//	//******** job1_3 **************	
//	public static class Reducer_MakeHESig extends Reducer<IntWritable,DouArr_ShortArr_SURFpoint_ShortArr,IntWritable,IntArr_HESig_ShortArr_Arr>{
//		//the same as Mapper_SURFVW_HE_TVector in MapRed_IndexPhotoFeat_3jobs
//		private boolean disp;
//		private double[][] centers;
//		private DenseMatrix64F pMatrix;
//		private double[][] HEThreshold;
//		private ArrayList<HashSet<Integer>> node_vw_links;
//		private ArrayList<double[]> nodes;
//		private int vws_NN;
////			private int nodes_NN;
//		private double alph_NNDist;
//		private float aveNum_vwNN;
//		private int procPhotos;
//		private int dispInter;
//		private long startTime, endTime;
//		
//		@SuppressWarnings("unchecked")
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			//***** read visual word cluster centers***//
//			BufferedReader intstr_data = new BufferedReader(new InputStreamReader(new FileInputStream("VWs.file"), "UTF-8"));
//		    String line1Photo; int centerNum=0; 
//		    while((line1Photo=intstr_data.readLine())!=null){ //line1Photo: VL-99983{n=3401 c=[-34.177, 153.369, .....] r=[7.903, 14.364, .....]}
//		    	centerNum++;
//			}intstr_data.close();
//			centers=new double[centerNum][];
//			intstr_data = new BufferedReader(new InputStreamReader(new FileInputStream("VWs.file"), "UTF-8"));
//			int lineindex=0;
//			while((line1Photo=intstr_data.readLine())!=null){ //line1Photo: VL-99983{n=3401 c=[-34.177, 153.369, .....] r=[7.903, 14.364, .....]}
//				centers[lineindex]=General.StrArrToDouArr(line1Photo.split("\\[")[1].split("\\]")[0].split(","));
//				lineindex++;
//			}intstr_data.close();
//			System.out.println("visual word numbers: "+centerNum);
//			
//			//***** read projection matrix P ***//
//			try {
//				pMatrix=(DenseMatrix64F) General.readObject("pMatrix.file");
//			} catch (ClassNotFoundException e) {
//				e.printStackTrace();
//			}
//			//***** read HEThreshold ***//
//			intstr_data = new BufferedReader(new InputStreamReader(new FileInputStream("HEThreshold.file"), "UTF-8"));
//			HEThreshold=new double[centerNum][];lineindex=0;
//			while((line1Photo=intstr_data.readLine())!=null){ 
//				HEThreshold[lineindex]=General.StrArrToDouArr(line1Photo.split(","));
//				lineindex++;
//			}intstr_data.close();
//			
//			//load middleNode
//			try {
//				nodes= (ArrayList<double[]>) General.readObject("middleNode.file");
//			} catch (ClassNotFoundException e) {
//				e.printStackTrace();
//			}
//			System.out.println("node number: "+nodes.size());
//			//load node_vw_links
//			try {
//				node_vw_links= (ArrayList<HashSet<Integer>>) General.readObject("nodeLink_learned.file");
//			} catch (ClassNotFoundException e) {
//				e.printStackTrace();
//			}
//			int maxLength=0;  int minLength=999999; 
//			for(int i=0;i<node_vw_links.size();i++){
//				maxLength=Math.max(node_vw_links.get(i).size(),maxLength);
//				minLength=Math.min(node_vw_links.get(i).size(),minLength);
//			}
//			System.out.println("node_vw_links, link number per node, maxLength: "+maxLength+", minLength:"+minLength);
//			
//			//set vws_NN
//			vws_NN=10;
////				////set nodes_NN
////				nodes_NN=4;
//			//set alph_NNDist
//			alph_NNDist=1.2;
//			//set aveNum_vwNN, aveNum of mult-assigned vwed for each feat
//			aveNum_vwNN=0;
//			//set procPhotos
//			procPhotos=0;
//			//set dispInter
//			dispInter=50;
//			startTime=System.currentTimeMillis(); //startTime
//			
//			System.out.println("reducer setup finsihed!");
//			disp=true; 
//			super.setup(context);
//	 	}
//		
//		protected void reduce(IntWritable key, Iterable<DouArr_ShortArr_SURFpoint_ShortArr> value, Context context) throws IOException, InterruptedException {
//			//key: photoName
//			//value: file content
//			//******** only one in value! ************	
//			int loopNum=0; DouArr_ShortArr_SURFpoint_ShortArr photoFeat_IOpoint=new DouArr_ShortArr_SURFpoint_ShortArr();
//			for(Iterator<DouArr_ShortArr_SURFpoint_ShortArr> it=value.iterator();it.hasNext();){// loop over all BufferedImage_jpg				
//				photoFeat_IOpoint=it.next();
//				loopNum++;
//			}
//			General.Assert(loopNum==1, "error in Reducer_SURFVW_HE_selectQurey! one photoName, one photo, loopNum should == 1, here loopNum="+loopNum);
//			
//			IntWritable mapout_Key=new IntWritable(-1);//"clusterId":0~19999
//			IntArr_HESig_ShortArr_Arr mapout_Value=new IntArr_HESig_ShortArr_Arr(); //"photoName&HESings", when use combiner, combiner's input/output class should be the same and equal to mapper's output class!!
//		
//			int photoName=key.get();// photoName
//			
//			procPhotos++;
//			
//			//***classify visual feat to visual word***//
//			double[][] photoFeat=photoFeat_IOpoint.douArrs.getRawArrArr(); SURFpoint[] interestPoints=photoFeat_IOpoint.points.getArr();
//			HashMap<Integer,ArrayList<SURFfeat>> VW_Sigs=new HashMap<Integer,ArrayList<SURFfeat>>();
//			int[] featStat=General_BoofCV.makeVW_HESig(VW_Sigs, photoFeat, interestPoints, centers,  pMatrix, HEThreshold, node_vw_links, nodes, vws_NN, alph_NNDist); //feat num, vw num, mutiAssNum, uniqueVW num
//			if (disp==true){ 
//	        	System.out.println("\t extract VW_Sigs for one Photo finished! photoName: "+photoName+", ori-feat num: "+featStat[0]+", vws num:"+featStat[1]
//	        			+", aveNum of mult-assigned vwed for each feat is:"+featStat[2]+", unique vw num: "+featStat[3]+",  "+General.dispTime(System.currentTimeMillis()-startTime, "s"));
//			}
//			aveNum_vwNN+=featStat[2];
//			//mapper outPut
//			for(Entry<Integer, ArrayList<SURFfeat>> oneEnt : VW_Sigs.entrySet()){
//				HESig[] sigs=General_BoofCV.SURFfeats_to_HESigs(oneEnt.getValue().toArray(new SURFfeat[0]));
//				//set key-value
//				mapout_Key.set(oneEnt.getKey());//vw
//				mapout_Value.set(new int[]{photoName}, new HESig[][]{sigs}); // photoName, SURFfeat
//		        context.write(mapout_Key, mapout_Value);
//			}
//			//debug disp info
//	        if (disp==true){ 
//				System.out.println("\t mapout_Key, vw: "+mapout_Key.toString());
//				System.out.println("\t mapout_Value, photoName: "+mapout_Value.getIntegers()[0]+", SURFfeats number: "+mapout_Value.getFeats()[0].getArr().length);
//				disp=false;
//				System.out.println("\t disp:"+disp);
//			}
//			//disp
//    		if((procPhotos)%dispInter==0){ 							
//				endTime=System.currentTimeMillis(); //end time 
//				System.out.println( "indexing photo feat, "+procPhotos+" photos finished!! ......"+ General.dispTime (endTime-startTime, "min"));
//			}
//		}
//		
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//		    endTime=System.currentTimeMillis(); //end time 
//			System.out.println("\n one reducer finished! total processed photos in this Reducer: "+procPhotos+" ....."+ General.dispTime ( endTime-startTime, "min"));
//			aveNum_vwNN/=procPhotos;
//			System.out.println("aveNum of mult-assigned vws for each feat is:"+aveNum_vwNN+", with vws_NN:"+vws_NN+", alph_NNDist:"+alph_NNDist);
//			super.setup(context);
//	 	}
//	}
//
//	//******** job2 **************	
//	public static class Reducer_SearchTVector extends Reducer<IntWritable,IntArr_HESig_ShortArr_Arr,Key_QID_VW,Int_MatchFeat_Arr>  {
//
//		private Configuration conf;
//		private FileSystem HDFS;
//		private String TVectorPath;
//		private int HMDistThr;
//		private float[] hammingW;
//		private int[] PaIDs;
//		private StringBuffer vws_queryNums;
//		private boolean disp;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			conf = context.getConfiguration();
//			HDFS=FileSystem.get(conf);
//			//***** set TVector MapFile Path***//
//			TVectorPath = "hdfs://p-head03.alley.sara.nl/user/yliu/"+conf.get("mapred.TVectorPath");
//			System.out.println("TVectorPath: "+TVectorPath);
//			//***** set Hamming distance threshold***//
//			HMDistThr=Integer.valueOf(conf.get("mapred.HMDistThr"));
//			System.out.println("HMDistThr: "+HMDistThr);
//			//***** set hammingW***//
//			int HSigBit=Integer.valueOf(conf.get("mapred.HSigBit")); 
//			HMDistThr=Integer.valueOf(conf.get("mapred.HMDistThr")); 
//			hammingW= General_BoofCV.make_HMWeigthts(HSigBit, HMDistThr);
//			System.out.println("HSigBit: "+HSigBit+", hammingW: "+General.floatArrToString(hammingW, "_", "0.00"));
//			//** set PaIDs **//
//			try {
//				PaIDs= (int[]) General.readObject("PaIDs.file");
//			} catch (ClassNotFoundException e) {
//				System.err.println("error in Partitioner_VW, load PaIDs from distributed cache fail, ClassNotFoundException!! ");
//				e.printStackTrace();
//			} catch (IOException e) {
//				System.err.println("error in Partitioner_VW, load PaIDs from distributed cache fail, IOException!! ");
//				e.printStackTrace();
//			}
//			System.out.println("PaIDs load finished, total partioned reducer number : "+(PaIDs[PaIDs.length-1]+1)+", job.setNumReduceTasks(jobRedNum) should == this value!!");
//			// ***** setup finsihed ***//
//			System.out.println("setup finsihed!");
//			vws_queryNums=new StringBuffer();
//			disp=true;
//			super.setup(context);
//	 	}
//		
//		@Override
//		public void reduce(IntWritable VW, Iterable<IntArr_HESig_ShortArr_Arr> QueryNames_feats, Context context) throws IOException, InterruptedException {
//			//QueryNameSigs: QueryName-Integer, Sigs:-ByteArrList
//
//			int progInter=500;  int vw=VW.get();
//			System.out.println("this reducer is for VW: "+VW);
//
//			//get group number for this vw
//			int groNum; 
//			if(vw==0){
//				groNum=PaIDs[vw]-(-1); //PaIDs value from 0!, PaIDs[-1] should be -1
//			}else{
//				groNum=PaIDs[vw]-PaIDs[vw-1];
//			}
//			System.out.println("total allocated reducers for this vw: "+groNum);
//			//******** set output key,value ************
//			Key_QID_VW redOutKey=new Key_QID_VW();
//			Int_MatchFeat_Arr redOutValue =new Int_MatchFeat_Arr();
//			
//			long startTime=System.currentTimeMillis();
//
//			//********set vw TVector(SeqFile)************
//			String TVPath=TVectorPath+"/part-r-"+General.StrleftPad(vw+"",0,5,"0");//1vw, 1 seqFile
//			System.out.println("TVPath: "+ TVPath);
//			ArrayList<Integer> TVector_docIDs=new ArrayList<Integer>(); ArrayList<SURFfeat[]> TVector_feats=new ArrayList<SURFfeat[]>();
//			System.out.println("before read TVector, current memory info: "+General.memoryInfo());
//			int TVectorFeatNum=General_BoofCV.readTVectorIntoMemory(HDFS, TVPath, conf, vw, TVector_docIDs, TVector_feats);
//			System.out.println("read this TVector into memory finished! docNum: "+ TVector_docIDs.size()+", featNum: "+TVectorFeatNum
//					+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s")+", current memory info: "+General.memoryInfo());
//			startTime=System.currentTimeMillis();
//			
//			//******* search TVector ***********
//			int queryNum_thisVW=0; int queryFeatNum_thisVW=0; HESig[] queryFeats;
//			//process querys have this vw
//        	for(Iterator<IntArr_HESig_ShortArr_Arr> it=QueryNames_feats.iterator();it.hasNext();){
//        		IntArr_HESig_ShortArr_Arr Querys=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//        		int queryNum_thisInterator=Querys.getIntegers().length;
//        		for(int query_i=0;query_i<queryNum_thisInterator;query_i++){
//	        		//********* process one query *********
//					int queryName=Querys.getIntegers()[query_i]; queryFeats=Querys.getFeats()[query_i].getArr();
//					redOutKey.set(queryName, vw);
//					//compare docs in TVector for this query
//					ArrayList<Integer> docs=new ArrayList<Integer>(); ArrayList<Float> scores=new ArrayList<Float>();
////					int matchDocNum=0; int matchFeatNum=0;
//					for(int doc_i=0;doc_i<TVector_docIDs.size();doc_i++){
//						//get match link and score
//						float hmScore=General_BoofCV.compare_HESigs(queryFeats, TVector_feats.get(doc_i), HMDistThr, hammingW);
//						if (hmScore>0) {
//							docs.add(TVector_docIDs.get(doc_i));scores.add(hmScore);
//						}
////						MatchFeat_ShortArr oneDocMatches= General_BoofCV.compare_HESigs(queryFeats, TVector_feats.get(doc_i), HMDistThr);
////						if (oneDocMatches!=null) { //this doc exist math for the vw of this query 
////							matchDocNum++; matchFeatNum+=oneDocMatches.getArr().length;
////							//outputfile: SequcenceFile; outputFormat: key->Key_QID_VW  value->Int_MatchFeat_ShortArr
////							redOutValue.set(TVector_docIDs.get(doc_i), oneDocMatches);
////							context.write(redOutKey, redOutValue); 	       
////							if(disp){//for debug
////								System.out.println("show one example: 1 doc's matches of 1 query --- queryName:"+queryName+", docID:"+TVector_docIDs.get(doc_i)+", vw:"+vw+", its Matches:"+oneDocMatches.getArr().length);
////								for (int i = 0; i < oneDocMatches.getArr().length; i++) {
////									System.out.println("\t match-"+i+": "+oneDocMatches.getArr()[i].toString());
////								}
////								disp=false;
////							}
////						}
//					}
//					//************ report progress ******************
//        			queryNum_thisVW++; queryFeatNum_thisVW+=queryFeats.length;
//					if(queryNum_thisVW%progInter==0){
//						long time=System.currentTimeMillis()-startTime;
//						System.out.println("curent total finished querys:"+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW
//								+", time:"+General.dispTime(time, "min")+", average compare time per sig pair:"+(double)time/TVectorFeatNum/queryFeatNum_thisVW);
//						System.out.println("--show info for current query: "+queryName+", sig number:"+queryFeats.length);
////						System.out.println("--matched docs number: "+matchDocNum+", tot matchFeatNum:"+matchFeatNum);
//						System.out.println("--current memory info: "+General.memoryInfo());
//						disp=true;
//					}
//
//        		}
//	        }
//        	
//        	//*** some statistic ********
//        	System.out.println("one reduce finished! total query number for this vw: "+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW);
//        	vws_queryNums.append(vw+"_"+queryNum_thisVW+"_"+queryFeatNum_thisVW+",");
//        	
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("Reducer finished! vws_queryNums_queryFeatNums: "+vws_queryNums.toString());
//			super.setup(context);
//	 	}
//	}
//
//	//******** job3 **************	
//	public static class Mapper_transfer extends Mapper<Key_QID_VW,Int_MatchFeat_Arr, Key_QID_DID, Int_MatchFeat_Arr>{
//
//		private boolean disp;
//		private int procSamples;
//
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			disp=true; 
//			procSamples=0;
//			// ***** setup finished ***//
//			System.out.println("Mapper_transfer setup finsihed!");
//			super.setup(context);
//	 	}
//		
//		@Override
//		protected void map(Key_QID_VW key, Int_MatchFeat_Arr value, Context context) throws IOException, InterruptedException {
//			//key: Key_QID_VW, value: Int_MatchFeat_ShortArr
//			int queryID=key.queryID;
//			int vw =key.vw;
//			//make query-doc keys
//			int docID=value.Integer;
//			MatchFeat_Arr docFeats=value.feats;
//			context.write(new Key_QID_DID(queryID, docID), new Int_MatchFeat_Arr(vw,docFeats));
//			
//
//			if (disp==true){ //debug disp info
//				System.out.println("Mapper_transfer: ");
//				System.out.println("queryName: "+queryID+", vw: "+vw+", docID:"+docID+", docFeats:"+docFeats.getArr().length);
//				disp=false;
//			}
//			//** output, set key, value **//
//			procSamples++;
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples);
//			super.setup(context);
//	 	}
//	}
//	
//	public static class Reducer_buildRank extends Reducer<Key_QID_DID,Int_MatchFeat_Arr,IntWritable,IntList_FloatList>  {
//		
//		private int topRank;
//		private int processedQueryNum;
//		private int reduceNum;
//		private long startTime;
//		private int dispInter;
//		
//		private MapFile.Reader queryFeatReader;
//		
//		private short[] doc_maxDim;
//		private float[] doc_BoVWVectorNorm;
//		
//		private double[][] scalingInfo_min_max;
//		private int HPM_level;
//		private int reRankByHPM;
//		private int HMDistThr;
//		private float[] hammingW;
//		private float[] idf_squre;
//		
//		private TreeSet<slave_masterFloat_DES<DocAllMatchFeats>> doc_scores_order;
//		private float thr;
//		private boolean isQuery1stDoc;
//		private int queryID_previous;
//		
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			//***** select top rank for output ***//
//			Configuration conf = context.getConfiguration();
//			topRank=Integer.valueOf(conf.get("mapred.topRank")); //select top rank as output
//			System.out.println("select top-"+topRank+" in rankList as output");
//			//***** read query feats ***//
//			queryFeatReader=General_Hadoop.openMapFileInNode("data", conf, true);
//			System.out.println("open query-Feat-MapFile finished");
//			//***** read doc info ***//
//			SequenceFile.Reader docInfoReader=General_Hadoop.openSeqFileInNode("docInfo.file", conf, true);
//			IntWritable docID=new IntWritable(); DocInfo docInfo=new DocInfo();
//			//get  totDocNum, maxDocID
//			int totDocNum=0; int maxDocID=0; 
//			while (docInfoReader.next(docID, docInfo)) {
//				totDocNum++;
//				maxDocID=Math.max(maxDocID, docID.get());
//			}docInfoReader.close();
//			System.out.println("total index photo number, totDocNum: "+totDocNum+", maxDocID:"+maxDocID);
//			//get doc_maxDim, doc_FeatVectorNormSqure
//			doc_maxDim=new short[maxDocID+1]; doc_BoVWVectorNorm=new float[maxDocID+1];
//			docInfoReader=General_Hadoop.openSeqFileInNode("docInfo.file", conf, false);
//			while (docInfoReader.next(docID, docInfo)) {
//				doc_maxDim[docID.get()]=(short) Math.max(docInfo.height, docInfo.width);
//				doc_BoVWVectorNorm[docID.get()]=docInfo.BoVWVectorNorm;
//			}docInfoReader.close();
//			//***** set scalingInfo_min_max for HPM ***//
//			scalingInfo_min_max=new double[][]{{-2*Math.PI,2*Math.PI},{Math.log10(0.1),Math.log10(10)},{0,0},{0,0}};//angle, scale, x, y
//			//***** set HPM_level for HPM ***//
//			HPM_level=Integer.valueOf(conf.get("mapred.HPM_level")); 
//			//***** set reRankByHPM for 1vs1&HPM rerank ***//
//			reRankByHPM=Integer.valueOf(conf.get("mapred.reRankByHPM")); //select top rank to do 1vs1 and HPM check
//			System.out.println("select top-"+reRankByHPM+" in the initial rankList to do 1vs1 and HPM check!");
//			//***** set hammingW***//
//			int HSigBit=Integer.valueOf(conf.get("mapred.HSigBit")); 
//			HMDistThr=Integer.valueOf(conf.get("mapred.HMDistThr")); 
//			hammingW= General_BoofCV.make_HMWeigthts(HSigBit, HMDistThr);
//			System.out.println("HSigBit: "+HSigBit+", hammingW: "+General.floatArrToString(hammingW, "_", "0.00"));
//			//***** idf_squre ***//
//			int[][] TVectorInfo = null;
//			try {
//				TVectorInfo=(int[][]) General.readObject("TVectorInfo.file"); //photoNum,featNum
//			} catch (ClassNotFoundException e) {
//				e.printStackTrace();
//			}
//			System.out.println("read int[][] TVectorInfo finished, total vw number:"+TVectorInfo.length);
//			idf_squre=General_BoofCV.make_idf_squre(TVectorInfo, totDocNum);
//			//***** set queryID_previous ***//
//			doc_scores_order=new TreeSet<slave_masterFloat_DES<DocAllMatchFeats>>(); //a fast way to get initial top-ranked docs
//			thr=0; isQuery1stDoc=true;
//			queryID_previous=-1;
//			// ***** setup finsihed ***//
//			processedQueryNum=0;
//			reduceNum=0;
//			startTime=System.currentTimeMillis();
//			dispInter=1;
//			System.out.println("setup finsihed!");
//			super.setup(context);
//	 	}
//			
//		@Override
//		public void reduce(Key_QID_DID QID_DID, Iterable<Int_MatchFeat_Arr> vw_MatchFeats, Context context) throws IOException, InterruptedException {
//			/**
//			 * 1 reduce: process 1 doc for 1 query, 
//			 */
//			
//			//key: query doc, value: vw and this doc's MatchFeats for this query
//
//			int thisQueryID=QID_DID.queryID;
//			//**************** check if new query ****************
//			if ((thisQueryID!=queryID_previous && reduceNum!=0)) {//new query and not the first query
//				//do 1vs1 and HPM check on previous query's initial rank
//				ArrayList<DocAllMatchFeats> doc_matches_previous=General_BoofCV.getDocsFromPreSel(doc_scores_order);
//				ArrayList<Integer> topDocs=new ArrayList<Integer>(); ArrayList<Float> topScores=new ArrayList<Float>();
//				General_BoofCV.rerank_InitialRank(doc_matches_previous, queryFeatReader, queryID_previous,
//						doc_BoVWVectorNorm, doc_maxDim, scalingInfo_min_max, HPM_level, hammingW, idf_squre,
//						topDocs, topScores, topRank,
//						true, System.currentTimeMillis(),"_1vs1AndHPM");
//				context.write(new IntWritable(queryID_previous), new IntList_FloatList(topDocs,topScores));
//				//disp progress
//				processedQueryNum++;
//				if (processedQueryNum%dispInter==0){ 
//					System.out.println(processedQueryNum+" querys finished! time:"+General.dispTime(System.currentTimeMillis()-startTime, "min")
//							+", current finished queryName: "+queryID_previous+", total listed photos in its initial rank: "+doc_scores_order.size()+", saved top doc numbers:"+topDocs.size()
//							+", top10Docs:"+topDocs.subList(0, Math.min(10, topDocs.size()))+", top10Scores:"+topScores.subList(0, Math.min(10, topDocs.size())));
//				}
//				//prepareForNext
//				doc_scores_order.clear();
//				thr=0; isQuery1stDoc=true;
//			}
//
//			//********combine all vw_MatchFeats for one doc************
//			int docID=QID_DID.docID;
//			ArrayList<Int_MatchFeat_Arr>  matches = new ArrayList<Int_MatchFeat_Arr>(); 
//			for(Iterator<Int_MatchFeat_Arr> it=vw_MatchFeats.iterator();it.hasNext();){
//				Int_MatchFeat_Arr oneVW_matches=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//				matches.add(new Int_MatchFeat_Arr(oneVW_matches.Integer,oneVW_matches.feats));
//			}
//			float initial_hmScore=General_BoofCV.make_Initial_DocMatchScore(matches, hammingW, idf_squre);//initial score doc only be hmDist, no 1vs1 check, no HPM check!
//			initial_hmScore/=doc_BoVWVectorNorm[docID]; //normlize by BoVWVectorNorm
//			//debug disp info for the 1st reduce function
//			if (isQuery1stDoc){ 
//				System.out.println("\t show one example for 1st reduce function(1st doc) of one query, queryID:"+thisQueryID+", docID:"+docID);
//				System.out.println("\t doc matches combine finished! total matched vws number: "+matches.size()+", initial initial_hmScore(only by hmDist):"+initial_hmScore);
//				System.out.println("\t time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms") );
//			}
//			
//			//**************** save this doc into doc_scores_order ****************
//	        if (isQuery1stDoc) {// if it is the first document:
//	        	thr = initial_hmScore;
//	        	isQuery1stDoc=false;
//	        }
//	        // if the array is not full yet:
//	        if (doc_scores_order.size() < reRankByHPM) {
//	        	doc_scores_order.add(new slave_masterFloat_DES<DocAllMatchFeats>(new DocAllMatchFeats(docID,matches.toArray(new Int_MatchFeat_Arr[0])),initial_hmScore));
//	            if (initial_hmScore<thr) //update current thr in doc_scores_order
//	            	thr = initial_hmScore;
//	        } else if (initial_hmScore>thr) { // if it is "better" than the least one in the current doc_scores_order
//	            // remove the last one ...
//	        	doc_scores_order.remove(doc_scores_order.first());
//	            // add the new one ...
//	        	doc_scores_order.add(new slave_masterFloat_DES<DocAllMatchFeats>(new DocAllMatchFeats(docID,matches.toArray(new Int_MatchFeat_Arr[0])),initial_hmScore));
//	            // update new thr in doc_scores_order
//	        	thr = doc_scores_order.first().getMaster();
//	        }   
//	        
//	        //update queryID_previous
//			queryID_previous=thisQueryID;
//			
//			reduceNum++;
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			//**** process last query ********
//			//do 1vs1 and HPM check on query's initial rank
//			ArrayList<DocAllMatchFeats> doc_matches_previous=General_BoofCV.getDocsFromPreSel(doc_scores_order);
//			ArrayList<Integer> topDocs=new ArrayList<Integer>(); ArrayList<Float> topScores=new ArrayList<Float>();
//			General_BoofCV.rerank_InitialRank(doc_matches_previous, queryFeatReader, queryID_previous,
//					doc_BoVWVectorNorm, doc_maxDim, scalingInfo_min_max, HPM_level, hammingW, idf_squre,
//					topDocs, topScores, topRank,
//					true, System.currentTimeMillis(),"_1vs1AndHPM");
//			context.write(new IntWritable(queryID_previous), new IntList_FloatList(topDocs,topScores));
//			System.out.println("last query: "+queryID_previous+", total listed photos in its initial rank: "+doc_scores_order.size()+", saved top doc numbers:"+topDocs.size()
//					+", top10Docs:"+topDocs.subList(0, Math.min(10, topDocs.size()))+", top10Scores:"+topScores.subList(0, Math.min(10, topDocs.size())));
//			processedQueryNum++;
//
//			// ***** finsihed ***//			
//			System.out.println("one Reducer finished! total querys in this reducer:"+processedQueryNum+", reduceNum:"+reduceNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
//			super.setup(context);
//	 	}
//	}
//
//}
