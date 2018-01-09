//package BuildRank;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
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
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//import MyAPI.General.General;
//import MyAPI.General.General_BoofCV;
//import MyAPI.General.General_Hadoop;
//import MyAPI.General.General_IR;
//import MyAPI.General.BoofCV.ComparePhotos_LocalFeature;
//import MyCustomedHaoop.Combiner.Combiner_combine_IntArr_HESig_ShortArr_Arr;
//import MyCustomedHaoop.KeyClass.Key_QID_DID;
//import MyCustomedHaoop.KeyClass.Key_QID_VW;
//import MyCustomedHaoop.Mapper.selectSamples.Mapper_selectSamples_hashMap;
//import MyCustomedHaoop.Partitioner.Partitioner_equalAssign_keyFrom0;
//import MyCustomedHaoop.Partitioner.Partitioner_forSearchTVector;
//import MyCustomedHaoop.Partitioner.Partitioner_random;
//import MyCustomedHaoop.Partitioner.Partitioner_random_sameKey;
//import MyCustomedHaoop.Partitioner.Partitioner_random_sameKey_PartKey;
//import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_1key_1value;
//import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
//import MyCustomedHaoop.ValueClass.DID_Score;
//import MyCustomedHaoop.ValueClass.DID_Score_Arr;
//import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;
//import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr_Arr;
//import MyCustomedHaoop.ValueClass.DocAllMatchFeats;
//import MyCustomedHaoop.ValueClass.DouArr_ShortArr;
//import MyCustomedHaoop.ValueClass.DouArr_ShortArr_SURFpoint_ShortArr;
//import MyCustomedHaoop.ValueClass.FloatArr;
//import MyCustomedHaoop.ValueClass.HESig;
//import MyCustomedHaoop.ValueClass.HESig_ShortArr;
//import MyCustomedHaoop.ValueClass.ImageRegionMatch;
//import MyCustomedHaoop.ValueClass.IntArr;
//import MyCustomedHaoop.ValueClass.IntArr_FloatArr;
//import MyCustomedHaoop.ValueClass.IntArr_HESig_ShortArr_Arr;
//import MyCustomedHaoop.ValueClass.IntList_FloatList;
//import MyCustomedHaoop.ValueClass.Int_MatchFeat_Arr;
//import MyCustomedHaoop.ValueClass.MatchFeat_Arr;
//import MyCustomedHaoop.ValueClass.SURFfeat;
//import MyCustomedHaoop.ValueClass.SURFpoint;
//import MyCustomedHaoop.ValueClass.SURFpoint_ShortArr;
//import MyCustomedHaoop.ValueClass.VW_DID_Score_Arr;
//import MyCustomedHaoop.ValueClass.VW_DID_Score_Arr_Arr;
//import MyCustomedHaoop.ValueClass.forTest;
//
//public class MapRed_buildRank_withTFIDFPreSel extends Configured implements Tool{
//
//	//code is not finished! as there are two many docs and scores (dataSize in TB) in the output from Reducer_buildInitialRank_TFIDF, this scheme actually takes very long time
//	//again, IO problem is the bottleneck! when no IO, each reduecer only 1min, when write out docID, socres, then it is 30mins+ !!
//	
//	/**
//	 * In Reducer_SearchTVector_getHMScore, it generate one DID_ScoreList Per VW, so takes 50+ TB for 30k query against 8.8M dataset
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
//	 * @param: "mapred.TVectorPath"  "mapred.HMDistThr_rankDoc" "mapred.VW_PartitionerIDs_Path"
//	 * 
//	 * job3: combine query_MatchFeat from each vw, build final rank for query 
//	 * @param: "mapred.topRank" "mapred.docInfoPath" "mapred.HPM_level" "mapred.HSigBit" "mapred.HMDistThr_rankDoc"
//	 * 
//	 * job4: save all querys' rank into one MapFile
//	 * 
//	 * 
//	 * 
//	 * @throws Exception 
//	 * @command_example: 
//	 * 
//	 * 3M:		hadoop jar BuildRank.jar BuildRank.MapRed_buildRank -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90.seq -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64 -Dmapred.HEThresholdPath=ImageR/HE_Thresholds64_VW20k_KMloop90.seq -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.seq -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=ICMR2013/Querys_100K_LtoS_from_D3M_ICMR2013.hashMap -Dmapred.TVectorPath=ImageR/TVector_3M_MapFile -Dmapred.HMDistThr=12 -Dmapred.docInfoPath=ImageR/photoFeatNum_3M -Dmapred.topRank=10000 1000 1000 1 3M_Photos_SeqFiles ImageR/SearchResult_D3M_Q100K_ICMR13 _rankDocScore
//	 * MEva13:	hadoop jar BuildRank.jar BuildRank.MapRed_buildRank -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90.seq -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64 -Dmapred.HEThresholdPath=ImageR/HE_Thresholds64_VW20k_KMloop90.seq -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.seq -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=MediaEval13/Querys/ -Dmapred.TVectorPath=MediaEval13/TVector_MEva13_9M_MapFile -Dmapred.HMDistThr=12 -Dmapred.docInfoPath=MediaEval13/photoFeatNum_MEva13_9M -Dmapred.topRank=10000 1000 1000 66M_Phos_Seqs MediaEval13/ranks/SURF_D9M_Q250K _rankDocScore
//	 * Herve:	hadoop jar BuildRank.jar BuildRank.MapRed_buildRank -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90.seq -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64 -Dmapred.HEThresholdPath=ImageR/HE_Thresholds64_VW20k_KMloop90.seq -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.seq -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/HerveImage/HerverImage_querys_transIndex_L_to_S.hashMap -Dmapred.TVectorPath=ImageR/BenchMark/HerveImage/TVector -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.docInfoPath=ImageR/BenchMark/HerveImage/docInfo -Dmapred.TVectorInfoPath=ImageR/BenchMark/HerveImage/TVectorInfo -Dmapred.VWFileInter=20 -Dmapred.HPM_level=6 -Dmapred.reRankByHPM=1000 -Dmapred.topRank=1000 720 720 720 ImageR/BenchMark/HerveImage/HerverImage_query.seq _Herve_1.5K ImageR/BenchMark/HerveImage/ranks/SURF _rankDocScore
//	 * Oxford:	hadoop jar BuildRank.jar BuildRank.MapRed_buildRank -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90.seq -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64 -Dmapred.HEThresholdPath=ImageR/HE_Thresholds64_VW20k_KMloop90.seq -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.seq -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/Oxford/OxfordBuilding_querys_transIndex_L_to_S.hashMap -Dmapred.TVectorPath=ImageR/BenchMark/Oxford/TVector -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.docInfoPath=ImageR/BenchMark/Oxford/docInfo -Dmapred.TVectorInfoPath=ImageR/BenchMark/Oxford/TVectorInfo -Dmapred.HPM_level=6 -Dmapred.reRankByHPM=1000 -Dmapred.topRank=1000 720 720 720 ImageR/BenchMark/Oxford/OxfordBuilding_query.seq _Oxford_5K ImageR/BenchMark/Oxford/ranks/SURF _rankDocScore
//	 * Oxford:	hadoop jar BuildRank.jar BuildRank.MapRed_buildRank -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/forVW/D_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW100k/loop-99/part-r-00000 -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64 -Dmapred.HEThresholdPath=ImageR/forVW/D_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho50k_HEThr64_VW100k_KMloop99/part-r-00000 -Dmapred.middleNode=ImageR/forVW/MiddleNode1000_onVW100k/loop-199/part-r-00000 -Dmapred.nodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100knode_vw_links_M1000_VW100k_I200.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/Oxford/OxfordBuilding_querys_transIndex_L_to_S.hashMap -Dmapred.TVectorPath=ImageR/BenchMark/Oxford/TVector -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.docInfoPath=ImageR/BenchMark/Oxford/docInfo -Dmapred.TVectorInfoPath=ImageR/BenchMark/Oxford/TVectorInfo -Dmapred.HPM_level=6 -Dmapred.reRankByHPM=1000 -Dmapred.topRank=1000 720 720 720 ImageR/BenchMark/Oxford/OxfordBuilding_query.seq _Oxford_5K ImageR/BenchMark/Oxford/ranks/SURF _rankDocScore
//	 * SanFran:	hadoop jar BuildRank.jar BuildRank.MapRed_buildRank -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90.seq -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64 -Dmapred.HEThresholdPath=ImageR/HE_Thresholds64_VW20k_KMloop90.seq -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.seq -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/SanFrancisco/SanFrancisco_querys_transIndex_L_to_S.hashMap -Dmapred.TVectorPath=ImageR/BenchMark/SanFrancisco/TVector -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.docInfoPath=ImageR/BenchMark/SanFrancisco/docInfo -Dmapred.TVectorInfoPath=ImageR/BenchMark/SanFrancisco/TVectorInfo -Dmapred.HPM_level=6 -Dmapred.reRankByHPM=1000 -Dmapred.topRank=1000 720 720 720 ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data _SanFrancisco ImageR/BenchMark/SanFrancisco/ranks/SURF _rankDocScore
//	 * TMM_GVR: hadoop jar BuildRank.jar BuildRank.MapRed_buildRank -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90.seq -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64 -Dmapred.HEThresholdPath=ImageR/HE_Thresholds64_VW20k_KMloop90.seq -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.seq -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=MediaEval13/Querys/ -Dmapred.TVectorPath_numOnly=TMM_GVR/imagR/TVectorNumOnly -Dmapred.TVectorPath=TMM_GVR/imagR/TVector -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.docInfoPath=TMM_GVR/imagR/docInfo -Dmapred.TVectorInfoPath=TMM_GVR/imagR/TVectorInfo -Dmapred.HPM_level=6 -Dmapred.reRankByHPM=1000 -Dmapred.topRank=1000 1000 2000 1000 66M_Phos_Seqs _MEva13_9M TMM_GVR/imagR/ranks/SURF _rankDocScore
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
//		int ret = ToolRunner.run(new MapRed_buildRank(), args);
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
//		int querySetSize=10*1000;//30*1000
//		HashMap<Integer, Integer> totQ=(HashMap<Integer, Integer>) General.readObject("O:/MediaEval13/MEval13_L_to_S_test.hashMap");
//		String subQSetFolder="O:/MediaEval13/Querys_perSubSet"+querySetSize/1000+"k/";
//		General.makeORdelectFolder(subQSetFolder);
//		Random rand=new Random();
//		ArrayList<HashMap<Integer, Integer>> Qsets =General.randSplitHashMap(rand, totQ, 0, querySetSize);
//		int totQnum=0;
//		for (int i = 0; i < Qsets.size(); i++) {
//			General.writeObject(subQSetFolder+"Q"+i, Qsets.get(i));
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
//	@SuppressWarnings("rawtypes")
//	@Override
//	public int run(String[] args) throws Exception {
//		Configuration conf = getConf();
//		FileSystem hdfs=FileSystem.get(conf);
//		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); //use this to parse args!
//		String homePath="hdfs://p-head03.alley.sara.nl/user/yliu/";
//		ArrayList<String> cacheFilePaths=new ArrayList<String>();
//		
//		//get vw_num
//		float[][] centers=General_Hadoop.readMatrixFromSeq_floatMatrix(conf, homePath+conf.get("mapred.VWPath"),hdfs);
//	    int vw_num=centers.length;
//	    //set VWFileInter
//	    int VWFileInter=conf.get("mapred.VWFileInter")==null?vw_num/1000:Integer.valueOf(conf.get("mapred.VWFileInter"));//by default VWFileInter=vw_num/1000
//	    conf.set("mapred.VWFileInter", VWFileInter+"");
//	    System.out.println("vw_num:"+vw_num+", VWFileInter:"+VWFileInter);
//	    
//		//set reducer number
//		int job1_1RedNum=Integer.valueOf(otherArgs[0]); //reducer number for extract query's SURF
//		System.out.println("job1_1RedNum:"+job1_1RedNum);
//		int job2_3RedNum=Integer.valueOf(otherArgs[1]); //reducer number for build ini rank
//		System.out.println("job2_3RedNum for build ini rank:"+job2_3RedNum);
//		int job5RedNum=Integer.valueOf(otherArgs[2]); //reducer number for combine query_MatchFeat from each vw, build final rank for query
//		System.out.println("job5RedNum:"+job5RedNum);
//		//set imagesPath
//		String imagesPath=otherArgs[3]; //input path
//		System.out.println("imagesPath:"+imagesPath);
//		ArrayList<Path> imageSeqPaths = General_Hadoop.addImgPathsFromMyDataSet(imagesPath);
//		System.out.println("imageSeqPaths:"+imageSeqPaths);
//		//set Index label
//		String indexLabel=otherArgs[4]; //_Oxford_1M
//		indexLabel+="_"+vw_num/1000+"K-VW";
//		System.out.println("indexLabel:"+indexLabel);
//		conf.set("mapred.TVectorPath_numOnly", conf.get("mapred.TVectorPath_numOnly")+indexLabel);
//		conf.set("mapred.TVectorPath", conf.get("mapred.TVectorPath")+indexLabel);
//		conf.set("mapred.docInfoPath", conf.get("mapred.docInfoPath")+indexLabel);
//		conf.set("mapred.TVectorInfoPath", conf.get("mapred.TVectorInfoPath")+indexLabel);
//		//set output path
//		String out=otherArgs[5]+indexLabel; //output path
//		System.out.println("out:"+out);
//		//set output rank format
//		String saveRankFormat=otherArgs[6];//_rankDocScore, _rankDocMatches
//		System.out.println("saveRankFormat:"+saveRankFormat);
//		boolean onlyScore=saveRankFormat.equalsIgnoreCase("_rankDocScore");//only save rank with docScore or save rank with doc's all matches with the query
//		Class<? extends Writable> saveRankClass=onlyScore?IntList_FloatList.class:DID_Score_ImageRegionMatch_ShortArr_Arr.class;
//		Class<? extends Reducer> saveRankReducer=onlyScore?Reducer_buildRank_final_saveScore.class:Reducer_buildRank_final_saveDocMatches.class;
//		//set reRankByHPM
//		String[] reRankByHPMs=conf.get("mapred.reRankByHPM").split(",");
//		System.out.println("reRankByHPMs:"+General.StrArrToStr(reRankByHPMs, ","));
//		//set maxIniRankLength
//		int maxIniRankLength=General.getMax_ind_val(General.StrArrToIntArr(reRankByHPMs))[1];
//		conf.set("mapred.maxIniRankLength", maxIniRankLength+"");
//		System.out.println("maxIniRankLength:"+maxIniRankLength);
//		//set rankLabel_common
//		String[] rankLabel_common={"_HDs"+conf.get("mapred.HMDistThr_selDoc")+"-HMW"+conf.get("mapred.HMWeight_deta"), 
//				"_ReR--notValid-changInReRankByHPMLoop",
//				"_HDr"+conf.get("mapred.HMDistThr_rankDoc"),
//				"_top"+Integer.valueOf(conf.get("mapred.topRank"))/1000+"K"};
//		//set rankLabel
//		String[] rankLabel={"_OriHE", "_1vs1", "_1vs1AndHPM"+conf.get("mapred.HPM_level")};
//		String[] rerankFlag={"_OriHE", "_1vs1", "_1vs1AndHPM"};
//		//set selected querys set
//		ArrayList<String> selQuerys=new ArrayList<String>(); 
//		String queryHashMapPath=homePath+conf.get("mapred.SelQuerys");
//		if (hdfs.isFile(new Path(queryHashMapPath))) {
//			selQuerys.add(queryHashMapPath);
//		}else {
//			FileStatus[] files= hdfs.listStatus(new Path(queryHashMapPath));
//			for (int i = 0; i < files.length; i++) {
//				selQuerys.add(files[i].getPath().toString());
//			}
//		}
//		System.out.println("selQuerys:\n"+selQuerys+"\n");
//				
//		//**********************    build rank  ************//
//		Path[][][] rankPaths=new Path[reRankByHPMs.length][rerankFlag.length][selQuerys.size()];
////		for (int i = 0; i < selQuerys.size(); i++) {
//			int i = 0;
//			String loopLabel="_Q"+i+"_"+(selQuerys.size()-1);		
//			//******* job1_1: extract query's SURF raw feats ******
//			String Out_job1_1=out+loopLabel+"_querySURFRaw";
////			//Distributed cache, add selectPhotosPath
////			cacheFilePaths.clear();
////			cacheFilePaths.add(selQuerys.get(i)+"#SelSamples.file"); //SelSamples path with symLink
////			General_Hadoop.Job(conf, imageSeqPaths.toArray(new Path[0]), Out_job1_1, "Job1_1_getRawFeats"+loopLabel, job1_1RedNum, 8, 10, true,
////					MapRed_buildRank.class, Mapper_selectSamples_hashMap.class, Partitioner_random.class,null,Reducer_ExtractSURF.class,
////					IntWritable.class, BufferedImage_jpg.class, IntWritable.class,DouArr_ShortArr_SURFpoint_ShortArr.class,
////					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
////					cacheFilePaths.toArray(new String[0]),null);
//			//******* job1_2: read query's SURF raw feats, save query_SURFpoint into MapFile ******
//			String Out_job1_2=out+loopLabel+"_querySURFpoint";
////			General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_1)}, Out_job1_2, "Job1_2_getSURFpoint"+loopLabel, 1, 8, 10, true,
////					MapRed_buildRank.class, null, null,null,Reducer_SaveQueryPoints.class,
////					IntWritable.class, DouArr_ShortArr_SURFpoint_ShortArr.class, IntWritable.class,SURFpoint_ShortArr.class,
////					SequenceFileInputFormat.class, MapFileOutputFormat.class, 1*1024*1024*1024L, 0,
////					null,null);
//			//******* job1_3: read query's SURF raw feats, make HESig ******
//			String Out_job1_3=out+loopLabel+"_queryHESig";
////			//Distributed cache, add VWPath, pMatrixPath, HEThresholdPath, middleNode, nodeLink_learned
////			cacheFilePaths.clear();
////			cacheFilePaths.add(homePath+conf.get("mapred.VWPath")+"#centers.file"); //VWs path with symLink
////			cacheFilePaths.add(homePath+conf.get("mapred.pMatrixPath")+"#pMatrix.file"); //VWs path with symLink
////			cacheFilePaths.add(homePath+conf.get("mapred.HEThresholdPath")+"#HEThreshold.file"); //VWs path with symLink
////			cacheFilePaths.add(homePath+conf.get("mapred.middleNode")+"#middleNodes.file"); //VWs path with symLink
////			cacheFilePaths.add(homePath+conf.get("mapred.nodeLink_learned")+"#nodeLink_learned.file"); //VWs path with symLink
////			General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_1)}, Out_job1_3, "Job1_3_getHESig"+loopLabel, job1_1RedNum, 8, 10, true,
////					MapRed_buildRank.class, null, Partitioner_random.class,null,Reducer_MakeHESig.class,
////					IntWritable.class, DouArr_ShortArr_SURFpoint_ShortArr.class, IntWritable.class,IntArr_HESig_ShortArr_Arr.class,
////					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
////					cacheFilePaths.toArray(new String[0]),null);
//			//******* job2_1: make VW_PartitionerIDs for partition reducers in Search TVector, no outPut, save VW_PartitionerIDs to mapred.VW_PartitionerIDs_Path ******
//			conf.set("mapred.VW_PartitionerIDs_Path", out+"_VW_PartitionerIDs"+loopLabel);
////			//add Distributed cache
////			cacheFilePaths.clear();
////			cacheFilePaths.add(homePath+conf.get("mapred.TVectorInfoPath")+"#TVectorInfo.file"); //TVectorInfo with symLink
////			General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_3)}, null, "Job2_1_makeVW_PartitionerIDs"+loopLabel, 1, 8, 10, true,
////					MapRed_buildRank.class, Mapper_countVW_PhotoNum.class, null, null, Reducer_makeVW_PartitionerIDs_forTFIDF.class,
////					IntWritable.class, IntWritable.class, IntWritable.class,IntWritable.class,
////					SequenceFileInputFormat.class, NullOutputFormat.class, 0, 0,
////					cacheFilePaths.toArray(new String[0]),null);
//			//set job2_2RedNum,job3_2RedNum,job4RedNum based on VW_PartitionerIDs
//			int[] PaIDs=(int[]) General_Hadoop.readObject_HDFS(hdfs, new Path(homePath+conf.get("mapred.VW_PartitionerIDs_Path")).toString());
//			int job2_2RedNum=General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs); //reducer number for seachTVector, PaIDs: values from 0!
//			int job4RedNum=job2_2RedNum; //reducer number for seachTVector, PaIDs: values from 0!
//			//******* job2_2: Search TVector, get query_doc TF-IDF-Score for each vw ******
//			String Out_job2_2=out+loopLabel+"_AllDocScores-TFIDF";
//			//add Distributed cache
//			cacheFilePaths.clear();
//			cacheFilePaths.add(homePath+conf.get("mapred.docInfoPath")+"/part-r-00000#docInfo.file"); //docInfo with symLink
//			cacheFilePaths.add(homePath+conf.get("mapred.TVectorInfoPath")+"#TVectorInfo.file"); //TVectorInfo with symLink
//			cacheFilePaths.add(homePath+conf.get("mapred.VW_PartitionerIDs_Path")+"#PaIDs.file"); //PaIDs with symLink
//			General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_3)}, Out_job2_2, "Job2_2_getDocScoreTFIDF"+loopLabel, job2_2RedNum, 8, 10, true,
//					MapRed_buildRank.class, null, Partitioner_forSearchTVector.class, Combiner_combine_IntArr_HESig_ShortArr_Arr.class, Reducer_SearchTVector_getTFIDFscore.class,
//					IntWritable.class, IntArr_HESig_ShortArr_Arr.class, IntWritable.class, IntArr_FloatArr.class,
//					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
//					cacheFilePaths.toArray(new String[0]),null);
////			//******* job2_3: combine query_MatchScore from each vw, build initial top ranked docs for query ******
////			String Out_job2_3=out+rankLabel_common[0]+loopLabel+"_allIniDocs"+maxIniRankLength+"_temp";
////			//add Distributed cache
////			cacheFilePaths.clear();			
////			cacheFilePaths.add(homePath+conf.get("mapred.docInfoPath")+"/part-r-00000#docInfo.file"); //docInfo with symLink
////			cacheFilePaths.add(homePath+conf.get("mapred.VW_PartitionerIDs_Path")+"#PaIDs.file"); //PaIDs with symLink for check duplicated VW
////			General_Hadoop.Job(conf, new Path[]{new Path(Out_job2_2)}, Out_job2_3, "job2_3_buildInitialRank"+loopLabel, job2_3RedNum, 8, 10, true,
////					MapRed_buildRank.class, null, Partitioner_random_sameKey.class, null, Reducer_buildInitialRank_HE.class,
////					IntWritable.class, VW_DID_Score_Arr_Arr.class, IntWritable.class,IntArr.class,
////					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
////					cacheFilePaths.toArray(new String[0]),null);
////			//******* job2_2: Search TVector, get query_doc hmScore for each vw ******
////			String Out_job2_2=out+rankLabel_common[0]+loopLabel+"_DocScores";
////			//add Distributed cache
////			cacheFilePaths.clear();
////			cacheFilePaths.add(homePath+conf.get("mapred.docInfoPath")+"/part-r-00000#docInfo.file"); //docInfo with symLink
////			cacheFilePaths.add(homePath+conf.get("mapred.TVectorInfoPath")+"#TVectorInfo.file"); //TVectorInfo with symLink
////			cacheFilePaths.add(homePath+conf.get("mapred.VW_PartitionerIDs_Path")+"#PaIDs.file"); //PaIDs with symLink
////			General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_3)}, Out_job2_2, "Job2_2_getDocScore"+loopLabel, job2_2RedNum, 8, 10, true,
////					MapRed_buildRank.class, null, Partitioner_forSearchTVector.class, Combiner_combine_IntArr_HESig_ShortArr_Arr.class, Reducer_SearchTVector_getHMScore.class,
////					IntWritable.class, IntArr_HESig_ShortArr_Arr.class, IntWritable.class, VW_DID_Score_Arr_Arr.class,
////					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
////					cacheFilePaths.toArray(new String[0]),null);
////			//******* job2_3: combine query_MatchScore from each vw, build initial top ranked docs for query ******
////			String Out_job2_3=out+rankLabel_common[0]+loopLabel+"_allIniDocs"+maxIniRankLength+"_temp";
////			//add Distributed cache
////			cacheFilePaths.clear();			
////			cacheFilePaths.add(homePath+conf.get("mapred.docInfoPath")+"/part-r-00000#docInfo.file"); //docInfo with symLink
////			cacheFilePaths.add(homePath+conf.get("mapred.VW_PartitionerIDs_Path")+"#PaIDs.file"); //PaIDs with symLink for check duplicated VW
////			General_Hadoop.Job(conf, new Path[]{new Path(Out_job2_2)}, Out_job2_3, "job2_3_buildInitialRank"+loopLabel, job2_3RedNum, 8, 10, true,
////					MapRed_buildRank.class, null, Partitioner_random_sameKey.class, null, Reducer_buildInitialRank_HE.class,
////					IntWritable.class, VW_DID_Score_Arr_Arr.class, IntWritable.class,IntArr.class,
////					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
////					cacheFilePaths.toArray(new String[0]),null);
////			//************** loop over different rerank length **************
////			for (int rer_i = 0; rer_i < reRankByHPMs.length; rer_i++) {
////				conf.set("mapred.reRankByHPM",reRankByHPMs[rer_i]);
////				rankLabel_common[1]="_ReR"+reRankByHPMs[rer_i];
////				//******* job3: combine result from job2_3, 1 reducer, mutiple reduce(vws), make vw_matchedDocs ******
////				int job3RedNum=PaIDs.length%VWFileInter==0?PaIDs.length/VWFileInter:PaIDs.length/VWFileInter+1;; //each reducer process multiple VWs
////				conf.set("mapred.reducerInter", VWFileInter+"");
////				conf.set("mapred.vw_iniDocsPath", out+rankLabel_common[0]+rankLabel_common[1]+loopLabel+"_iniDocs");
////				General_Hadoop.Job(conf, new Path[]{new Path(Out_job2_3)}, null, "job3_groupVWQIDDocIDs"+loopLabel, job3RedNum, 8, 10, false,
////						MapRed_buildRank.class, Mapper_selectTopRankDocs.class, Partitioner_equalAssign_keyFrom0.class,null,Reducer_groupVW_QID_DocIDs.class,
////						IntWritable.class,IntArr.class,IntWritable.class,IntArr.class,
////						SequenceFileInputFormat.class, NullOutputFormat.class, 1*1024*1024*1024L, 0,
////						null,null);
////				//******* job4: Search TVector, get query_selectedDoc MatchFeat for each vw ******
////				String Out_job4=out+rankLabel_common[0]+rankLabel_common[1]+rankLabel_common[2]+loopLabel+"_MatchFeat";
////				//add Distributed cache
////				cacheFilePaths.clear();
////				cacheFilePaths.add(homePath+conf.get("mapred.TVectorInfoPath")+"#TVectorInfo.file"); //TVectorInfo with symLink
////				cacheFilePaths.add(homePath+conf.get("mapred.VW_PartitionerIDs_Path")+"#PaIDs.file"); //PaIDs with symLink
////				General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_3)}, Out_job4, "Job4_getMatchFeat"+loopLabel, job4RedNum, 8, 10, true,
////						MapRed_buildRank.class, null, Partitioner_forSearchTVector.class, Combiner_combine_IntArr_HESig_ShortArr_Arr.class, Reducer_SearchTVector_getDocFeat.class,
////						IntWritable.class,IntArr_HESig_ShortArr_Arr.class,Key_QID_VW.class,Int_MatchFeat_Arr.class,
////						SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
////						cacheFilePaths.toArray(new String[0]),null);
////				//******* job5: combine query_MatchFeat from each vw, build final rank for query ******
////				for (int j = 0; j < rerankFlag.length; j++) {//run for different rerank strategy
////					String Out_job5=out+rankLabel_common[0]+rankLabel_common[1]+rankLabel_common[2]+rankLabel_common[3]+rankLabel[j]+loopLabel+saveRankFormat;
////					conf.set("mapred.rerankFlag", rerankFlag[j]);
////					//add Distributed cache
////					cacheFilePaths.clear();
////					cacheFilePaths.add(homePath+Out_job1_2+"/part-r-00000/data#data"); //queryFeat_MapFile data path with symLink
////					cacheFilePaths.add(homePath+Out_job1_2+"/part-r-00000/index#index"); //queryFeat_MapFile index path with symLink
////					cacheFilePaths.add(homePath+conf.get("mapred.docInfoPath")+"/part-r-00000#docInfo.file"); //docInfo with symLink
////					cacheFilePaths.add(homePath+conf.get("mapred.TVectorInfoPath")+"#TVectorInfo.file"); //TVectorInfo with symLink
////					General_Hadoop.Job(conf, new Path[]{new Path(Out_job4)}, Out_job5, "buildRank"+rerankFlag[j]+loopLabel, job5RedNum, 8, 10, true,
////							MapRed_buildRank.class, Mapper_transfer_finalRank.class, Partitioner_random_sameKey_PartKey.class,null,saveRankReducer,
////							Key_QID_DID.class, Int_MatchFeat_Arr.class, IntWritable.class,saveRankClass,
////							SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
////							cacheFilePaths.toArray(new String[0]),null);	
////					rankPaths[rer_i][j][i]=new Path(Out_job5);
////				}
////				//clean-up
////				hdfs.delete(new Path(homePath+conf.get("mapred.vw_iniDocsPath")), true);//job 3
////				hdfs.delete(new Path(Out_job4), true);
////			}
////			//clean-up
////			hdfs.delete(new Path(Out_job1_1), true);
////			hdfs.delete(new Path(Out_job1_2), true);
////			hdfs.delete(new Path(Out_job1_3), true);
////			hdfs.delete(new Path(homePath+conf.get("mapred.VW_PartitionerIDs_Path")), true);
////			hdfs.delete(new Path(Out_job2_2), true);//docScores
////			hdfs.delete(new Path(Out_job2_3), true);
////		}
////		//******* job4: save all querys' rank into one MapFile ******
////		for (int rer_i = 0; rer_i < reRankByHPMs.length; rer_i++) {
////			rankLabel_common[1]="_ReR"+reRankByHPMs[rer_i];
////			for (int j = 0; j < rerankFlag.length; j++) {//run for different rerank strategy
////				General_Hadoop.Job(conf, rankPaths[rer_i][j], out+rankLabel_common[0]+rankLabel_common[1]+rankLabel_common[2]+rankLabel_common[3]+rankLabel[j]+saveRankFormat, "combine&save_"+rerankFlag[j], 1, 8, 10, true,
////						MapRed_buildRank.class, null, null, null, Reducer_InOut_1key_1value.class,
////						IntWritable.class, IntList_FloatList.class, IntWritable.class,saveRankClass,
////						SequenceFileInputFormat.class, MapFileOutputFormat.class, 0, 10,
////						null,null);	
////			}
////		}
////		//clean-up rankPaths
////		for (Path[][] pathss : rankPaths) {
////			for (Path[] paths: pathss) {
////				for (Path path: paths) {
////					hdfs.delete(path, true);
////				}
////			}
////		}
//		hdfs.close();
//		return 0;
//	}
//	
//	//******** job1_1 **************	
//	public static class Reducer_ExtractSURF extends Reducer<IntWritable,BufferedImage_jpg,IntWritable,DouArr_ShortArr_SURFpoint_ShortArr>{
//		//Reducer_extractSURF: extract double[][] feats, and SURFfeat_noSig[]
//		private ComparePhotos_LocalFeature comparePhotos;
//		private boolean disp;
//		private int procPhotos;
//		private int totFeatNum;
//		private int dispInter;
//		private long startTime;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			comparePhotos=new ComparePhotos_LocalFeature();
//			//set procPhotos
//			procPhotos=0;
//			totFeatNum=0;
//			//set dispInter
//			dispInter=30;
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
//			int photoName=key.get();// photoName
//			//******** only one in value! ************	
//			int loopNum=0; BufferedImage_jpg photo=new BufferedImage_jpg();
//			for(Iterator<BufferedImage_jpg> it=value.iterator();it.hasNext();){// loop over all BufferedImage_jpg				
//				photo=it.next();
//				loopNum++;
//			}
//			General.Assert(loopNum==1, "error in Reducer_SURFVW_HE_selectQurey! one photoName, one photo, loopNum should == 1, here loopNum="+loopNum);
//			//disp
//			procPhotos++;
//    		if((procPhotos)%dispInter==0){ 							
//				System.out.println( "extractSURF photo feat, "+procPhotos+" photos finished!! ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
//				disp=true;
//			}
//			//***classify visual feat to visual word***//
//			ArrayList<SURFpoint> interestPoints=new ArrayList<SURFpoint>();
//			double[][] photoFeat=comparePhotos.computeSURF_boofCV_09(photo.getBufferedImage(), interestPoints);
//			if(photoFeat!=null){ // photo has feat(some photos are too small, do not have interest point)
//				//mapper outPut
//				context.write(key, new DouArr_ShortArr_SURFpoint_ShortArr(new DouArr_ShortArr(photoFeat), new SURFpoint_ShortArr(interestPoints)));
//				//debug disp info
//		        if (disp==true){ 
//					System.out.println("\t show one example: ");
//					System.out.println("\t mapout_Key, photoName: "+photoName);
//					System.out.println("\t mapout_Value, photoFeat: number, "+photoFeat.length);
//					disp=false;
//				}
//		        totFeatNum+=photoFeat.length;
//			}else{
//				System.err.println("image exist, but no feat for photo: "+photoName);
//				return;
//			}
//			
//		}
//
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("\n one reducer finished! total processed photos in this Reducer: "+procPhotos+", totFeatNum:"+totFeatNum+" ....."+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
//			super.setup(context);
//	 	}
//	}
//
//	//******** job1_2 **************	
//	public static class Reducer_SaveQueryPoints extends Reducer<IntWritable,DouArr_ShortArr_SURFpoint_ShortArr,IntWritable,SURFpoint_ShortArr>{
//		private boolean disp;
//		private int procPhotos;
//		private int dispInter;
//		private long startTime;
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
//			//disp
//	        procPhotos++;
//    		if((procPhotos)%dispInter==0){ 							
//				System.out.println( "Save Query photo Points, "+procPhotos+" photos finished!! ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
//				disp=true;
//    		}
//			//mapper outPut
//			IntWritable mapout_Key=key;//photoName
//			SURFpoint_ShortArr mapout_Value=photoFeat_IOpoint.points; //SURFpoints
//			context.write(mapout_Key, mapout_Value);
//			//debug disp info
//	        if (disp==true){ 
//	        	System.out.println("\t show one example: ");
//				System.out.println("\t mapout_Key, photoName: "+mapout_Key.toString());
//				System.out.println("\t mapout_Value, SURFpoints: number_"+mapout_Value.getArr().length);
//				disp=false;
//			}
//			
//		}
//		
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("\n one reducer finished! total processed photos in this Reducer: "+procPhotos+" ....."+ General.dispTime ( System.currentTimeMillis()-startTime, "min"));
//			super.setup(context);
//	 	}
//	}
//		
//	//******** job1_3 **************	
//	public static class Reducer_MakeHESig extends Reducer<IntWritable,DouArr_ShortArr_SURFpoint_ShortArr,IntWritable,IntArr_HESig_ShortArr_Arr>{
//		private ComparePhotos_LocalFeature comparePhotos;
//		private boolean disp;
//		private float aveNum_vwNN;
//		private int procPhotos;
//		private int dispInter;
//		private long startTime, endTime;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf=context.getConfiguration();
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("centers.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//setup ComparePhotos_LocalFeature
//			comparePhotos=new ComparePhotos_LocalFeature();
//			System.out.println("current memory:"+General.memoryInfo());
//			//-setup_extractFeat
//			String info_setup_extractFeat= comparePhotos.setup_extractFeat("centers.file","pMatrix.file","HEThreshold.file","middleNodes.file","nodeLink_learned.file",10,1.2,conf);
//			System.out.println(info_setup_extractFeat);
//			System.out.println("current memory:"+General.memoryInfo());
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
//			int photoName=key.get();// photoName
//			//******** only one in value! ************	
//			int loopNum=0; DouArr_ShortArr_SURFpoint_ShortArr photoFeat_IOpoint=new DouArr_ShortArr_SURFpoint_ShortArr();
//			for(Iterator<DouArr_ShortArr_SURFpoint_ShortArr> it=value.iterator();it.hasNext();){// loop over all BufferedImage_jpg				
//				photoFeat_IOpoint=it.next();
//				loopNum++;
//			}
//			General.Assert(loopNum==1, "error in Reducer_SURFVW_HE_selectQurey! one photoName, one photo, loopNum should == 1, here loopNum="+loopNum);
//			//disp
//			procPhotos++;
//    		if((procPhotos)%dispInter==0){ 							
//				System.out.println( "indexing photo feat, "+procPhotos+" photos finished!! ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
//				disp=true;
//    		}
//			IntWritable mapout_Key=new IntWritable(-1);//"clusterId":0~19999
//			IntArr_HESig_ShortArr_Arr mapout_Value=new IntArr_HESig_ShortArr_Arr(); //"photoName&HESings", when use combiner, combiner's input/output class should be the same and equal to mapper's output class!!
//			
//			//***classify visual feat to visual word***//
//			double[][] photoFeat=photoFeat_IOpoint.douArrs.getRawArrArr(); SURFpoint[] interestPoints=photoFeat_IOpoint.points.getArr();
//			HashMap<Integer,ArrayList<SURFfeat>> VW_Sigs=new HashMap<Integer,ArrayList<SURFfeat>>();
//			int[] featStat=comparePhotos.makeVW_HESig(VW_Sigs, photoFeat, interestPoints); //feat num, vw num, mutiAssNum, uniqueVW num
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
//			}
//			
//		}
//		
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//		    endTime=System.currentTimeMillis(); //end time 
//			System.out.println("\n one reducer finished! total processed photos in this Reducer: "+procPhotos+" ....."+ General.dispTime ( endTime-startTime, "min"));
//			aveNum_vwNN/=procPhotos;
//			System.out.println("aveNum of mult-assigned vws for each feat is:"+aveNum_vwNN+", with vws_NN:"+comparePhotos.vws_NN+", alph_NNDist:"+comparePhotos.alph_NNDist);
//			super.setup(context);
//	 	}
//	}
//
//	//******** job2_1 **************	
//	public static class Mapper_countVW_PhotoNum extends Mapper<IntWritable, IntArr_HESig_ShortArr_Arr, IntWritable, IntWritable>{
//
//		private int procSamples;
//		private int progInter;
//
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			procSamples=0;
//			progInter=1000;
//			// ***** setup finished ***//
//			System.out.println("Mapper_transfer setup finsihed!");
//			super.setup(context);
//	 	}
//		
//		@Override
//		protected void map(IntWritable key, IntArr_HESig_ShortArr_Arr value, Context context) throws IOException, InterruptedException {
//			//key: VW, value: queryIDs, Feats
//			int vw =key.get();
//			int queryPhotoNum=value.getFeats().length;
//			context.write(key, new IntWritable(queryPhotoNum));
//			
//			procSamples++;
//			if(procSamples%progInter==0){ //debug disp info
//				System.out.println(procSamples+" vw_queryIDs_feats-samples finished: ");
//				System.out.println("--current finished vw: "+vw+", queryPhotoNum:"+queryPhotoNum);
//			}
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
//	public static class Reducer_makeVW_PartitionerIDs_forTFIDF extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>  {
//		
//		private int[][] TVectorInfo;
//		private int[] vw_photoNum;
//		private String VW_PartitionerIDs_Path;
//		private int procSamples;
//		private int progInter;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf = context.getConfiguration();
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("TVectorInfo.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//***** TVectorInfo ***//
//			TVectorInfo=(int[][]) General.readObject("TVectorInfo.file"); //photoNum,featNum
//			System.out.println("read int[][] TVectorInfo finished, total vw number:"+TVectorInfo.length);
//			//** set vw_featNum **//
//			vw_photoNum=new int[TVectorInfo.length];
//			//** set VW_PartitionerIDs_Path **//
//			VW_PartitionerIDs_Path=("hdfs://p-head03.alley.sara.nl/user/yliu/"+conf.get("mapred.VW_PartitionerIDs_Path"));
//			System.out.println("VW_PartitionerIDs_Path setted: "+VW_PartitionerIDs_Path);
//			// ***** setup finsihed ***//
//			procSamples=0;
//			progInter=1000;
//			System.out.println("setup finsihed!");
//			super.setup(context);
//	 	}
//		
//		@Override
//		public void reduce(IntWritable VW, Iterable<IntWritable> queryPhotoNum, Context context) throws IOException, InterruptedException {
//			//QueryNameSigs: QueryName-Integer, Sigs:-ByteArrList
//
//			int vw=VW.get();
//			
//			//******** get photoNum ************	
//			int loopNum=0; 
//			for(Iterator<IntWritable> it=queryPhotoNum.iterator();it.hasNext();){// loop over all BufferedImage_jpg				
//				IntWritable temp=it.next();
//				vw_photoNum[vw]+=temp.get();
//				loopNum++;
//			}
//			
//			procSamples++;
//			if(procSamples%progInter==0){ //debug disp info
//				System.out.println(procSamples+" vw_queryFeatNum-samples finished: ");
//				System.out.println("--current finished vw: "+vw+", queryPhotoNum:"+vw_photoNum[vw]+", loopNum:"+loopNum);
//			}
//			
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** make  PaIDs ***//
//			int[] PaIDs=General_Hadoop.make_vwPartitionIDs_TFIDF(TVectorInfo, vw_photoNum,700);
//			System.out.println("\n Reducer for make vwPartitionIDs finished, save it as int[] to VW_PartitionerIDs_Path! total partioned reducer number : "+General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs)+", job.setNumReduceTasks(jobRedNum) should == this value!!");
//			int maxReducerNum=0; int maxReducerNum_vw=0;
//			for (int i = 0; i < PaIDs.length; i++) {
//				int reducerNum=General_Hadoop.getVWReducerNum_from_vwPartitionIDs(PaIDs, i);
//				if(maxReducerNum<reducerNum){
//					maxReducerNum=reducerNum;
//					maxReducerNum_vw=i;
//				}
//			}
//			System.out.println("vw:"+maxReducerNum_vw+" has max reducerNum:"+maxReducerNum+", its TVector-featNum:"+TVectorInfo[maxReducerNum_vw][1]+", querys-PhotoNum:"+vw_photoNum[maxReducerNum_vw]);
//			// ***** write-out ***//
//			Configuration conf = context.getConfiguration();
//			FileSystem HDFS=FileSystem.get(conf);
//			General_Hadoop.writeObject_HDFS(HDFS, VW_PartitionerIDs_Path, PaIDs);
//			super.setup(context);
//	 	}
//	}
//
//	//******** job2_2 **************	
//	public static class Reducer_SearchTVector_getTFIDFscore extends Reducer<IntWritable,IntArr_HESig_ShortArr_Arr,IntWritable,IntArr_FloatArr>  {
//
//		private Configuration conf;
//		private FileSystem HDFS;
//		private ComparePhotos_LocalFeature comparePhotos;
//		private int totQueryNum;
//		private HashSet<Integer> uniqueQ;
//		private int VWFileInter;
//		private String TVectorPath_numOnly;
//		private int[] PaIDs;
//		private StringBuffer vws_queryNums;
//		private int reduceNum;
//		private int dispInter_reduce;
//		private boolean disp;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			conf = context.getConfiguration();
//			HDFS=FileSystem.get(conf);
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("PaIDs.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//***** set TVector SeqFile Path***//
//			VWFileInter=Integer.valueOf(conf.get("mapred.VWFileInter"));
//			TVectorPath_numOnly = "hdfs://p-head03.alley.sara.nl/user/yliu/"+conf.get("mapred.TVectorPath_numOnly");
//			System.out.println("TVectorPath_numOnly:"+TVectorPath_numOnly+", VWFileInter:"+VWFileInter);
//			//********** setup ComparePhotos_LocalFeature ***************
//			comparePhotos=new ComparePhotos_LocalFeature();
//			System.out.println("current memory:"+General.memoryInfo());
//			//-setup_scoreDoc
//			String info_setup_scoreDoc= comparePhotos.setup_scoreDoc(-1, -1, 
//					-1, "docInfo.file", "TVectorInfo.file", conf);
//			System.out.println(info_setup_scoreDoc);
//			System.out.println("current memory:"+General.memoryInfo());
//			//*************** set totQueryNum, uniqueQ ***************
//			totQueryNum=0;
//			uniqueQ=new HashSet<Integer>();
//			//** set PaIDs **//
//			PaIDs= (int[]) General.readObject("PaIDs.file");
//			System.out.println("PaIDs load finished, total partioned reducer number : "+General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs)+", job.setNumReduceTasks(jobRedNum) should == this value!!");
//			// ***** setup finsihed ***//
//			System.out.println("setup finsihed!");
//			vws_queryNums=new StringBuffer();
//			reduceNum=0;
//			dispInter_reduce=1;
//			disp=false;
//			super.setup(context);
//	 	}
//		
//		@Override
//		public void reduce(IntWritable VW, Iterable<IntArr_HESig_ShortArr_Arr> QueryNames_feats, Context context) throws IOException, InterruptedException {
//			//QueryNameSigs: QueryName-Integer, Sigs:-ByteArrList
//
//			int progInter=1000;  int vw=VW.get();
//
//			reduceNum++;
//			if (reduceNum%dispInter_reduce==0) {
//				disp=true;
//			}
//			float idf_squre=comparePhotos.idf_squre[vw];
//			General.dispInfo_ifNeed(disp, "\n this reduce is for VW: "+VW+", its idf_squre:"+idf_squre+", total allocated reducers for this vw: "+General_Hadoop.getVWReducerNum_from_vwPartitionIDs(PaIDs, vw));
//			
//			long startTime=System.currentTimeMillis();
//			int queryNum_thisVW=0; int queryFeatNum_thisVW=0; int TVectorDocNum=0; int TVectorFeatNum=0;
//			
//			//********read TVector(SeqFile) into memory************
//			String TVPath=General_Hadoop.getOneVWFilePath(TVectorPath_numOnly, vw, VWFileInter);//1vw, 1 seqFile
//			if (HDFS.exists(new Path(TVPath))) {
//				General.dispInfo_ifNeed(disp, "TVPath exists: "+ TVPath+", before read TVector_numOnly, current memory info: "+General.memoryInfo());
//				ArrayList<Integer> TVector_docIDs=new ArrayList<Integer>(1000*1000); ArrayList<Integer> TVector_featNums=new ArrayList<Integer>(1000*1000);
//				TVectorFeatNum=General_BoofCV.readTVectorNumOnlyIntoMemory(HDFS, TVPath, conf, vw, TVector_docIDs, TVector_featNums);
//				TVectorDocNum=TVector_docIDs.size();
//				General.dispInfo_ifNeed(disp, "read this TVector into memory finished! docNum: "+ TVectorDocNum+", featNum: "+TVectorFeatNum
//						+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s")+", current memory info: "+General.memoryInfo());
//				//******* search TVector ***********
//				HashSet<Integer> checkDupliQuerys=new HashSet<Integer>();
//				startTime=System.currentTimeMillis();
//				//process querys have this vw
//	        	for(Iterator<IntArr_HESig_ShortArr_Arr> it=QueryNames_feats.iterator();it.hasNext();){
//	        		IntArr_HESig_ShortArr_Arr Querys=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//	        		int queryNum_thisInterator=Querys.getIntegers().length;
//	        		for(int query_i=0;query_i<queryNum_thisInterator;query_i++){
//		        		//********* process one query *********
//						int queryName=Querys.getIntegers()[query_i]; int queryFeatNum=Querys.getFeats()[query_i].getArr().length;
//						General.Assert(checkDupliQuerys.add(queryName), "err! duplicate querys for VW:"+vw+", duplicate query:"+queryName);
//						//compare docs in TVector for this query
//						float commonScore=queryFeatNum*idf_squre; float[] docScores_thisVW=new float[TVector_featNums.size()];
//						for(int doc_i=0;doc_i<TVector_featNums.size();doc_i++){
//							//get match link and score
//							docScores_thisVW[doc_i]=TVector_featNums.get(doc_i)*commonScore;
//						}
////						context.write(new IntWritable(queryName), new IntArr_FloatArr(TVector_docIDs, docScores_thisVW));
//						//************ report progress ******************
//						queryNum_thisVW++; queryFeatNum_thisVW+=queryFeatNum;
//						if(disp==true && queryNum_thisVW%progInter==0){
//							long time=System.currentTimeMillis()-startTime;
//							System.out.println("\t --curent queryName:"+queryName+", finished querys for this vw:"+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW
//									+", time:"+General.dispTime(time, "min")+", average time per muitple operation:"+(double)time/TVectorDocNum/queryNum_thisVW);
//							System.out.println("\t ----current memory info: "+General.memoryInfo());
//						}
//	        		}
//		        }
//	        	uniqueQ.addAll(checkDupliQuerys);
//			}else {
//				System.out.println("\n -Warning!!  for VW-"+vw+", query exist this vw, but no photo in dataset exist this vw, TVector not exist for this vw!");
//			}
//        	//*** some statistic ********
//			totQueryNum+=queryNum_thisVW;
//        	General.dispInfo_ifNeed(disp, "one reduce finished! total query number for this vw: "+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//        	disp=false;
//        	
//        	vws_queryNums.append(vw+"_"+queryNum_thisVW+"_"+queryFeatNum_thisVW+"_"+TVectorDocNum+"_"+TVectorFeatNum+", ");
//        	
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("\n Reducer finished! reduceNum(vws num): "+reduceNum+", unique query num:"+uniqueQ.size()+", tot query num:"+totQueryNum
//					+", vws_queryNums_queryFeatNums_TVectorDocNum_TVectorFeatNum: "+vws_queryNums.toString());
//			super.setup(context);
//	 	}
//	}
//
//	//******** job2_3 **************	
//	public static class Reducer_buildInitialRank_TFIDF extends Reducer<IntWritable,DID_Score_Arr,IntWritable,IntArr>  {
//		Configuration conf;
//		private ComparePhotos_LocalFeature comparePhotos;
//		
//		private int maxIniRankLength;
//		
//		private Path tempPathFor_VWDocs; //save vw_docs to seqFile on local node
//		
//		private int[] PaIDs;
//		private int totRedNumForSearchTVector;
//		private Partitioner_forSearchTVector<IntWritable> partitioner_forSearchTVector;
//		
//		private int processedQueryNum;
//		private long startTime;
//		private int dispInter;
//		private boolean disp;
//	
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			conf = context.getConfiguration();	
//			//setup ComparePhotos_LocalFeature
//			comparePhotos=new ComparePhotos_LocalFeature();
//			System.out.println("current memory:"+General.memoryInfo());
//			//-setup_scoreDoc
//			String info_setup_scoreDoc= comparePhotos.setup_scoreDoc(Integer.valueOf(conf.get("mapred.HMDistThr_selDoc")), Double.valueOf(conf.get("mapred.HMWeight_deta")), 
//					-1, "docInfo.file", null, conf);
//			System.out.println(info_setup_scoreDoc);
//			System.out.println("current memory:"+General.memoryInfo());
//			//***** set maxIniRankLength for further rerank ***//
//			maxIniRankLength=Integer.valueOf(conf.get("mapred.maxIniRankLength")); //select top rank to do 1vs1 and HPM check
//			System.out.println("select top-"+maxIniRankLength+" in the initial rankList to do further rerank: 1vs1 and HPM check!");
//			//***** set tempPathFor_VWDocs for save vw_docs to SeqFile ***//
//			tempPathFor_VWDocs= new Path("vw_docs.seq");
//			String[] info=General_Hadoop.checkFileInNode("vw_docs.seq");//absoultPath, parentPath, files in parentPath
//			System.out.println("tempPathFor_VWDocs for save vw_docs setted: "+tempPathFor_VWDocs+", its absolut path in local-node: \n"+info[0]);
//			System.out.println("files in its parent path: \n"+info[2]);
//			// ***** setup PaID for check duplicate-VW ***//
//			PaIDs= (int[]) General.readObject("PaIDs.file");//each element in PaIDs is mutipled by 10!
//			totRedNumForSearchTVector=General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs); //reducer number for seachTVector, PaIDs: values from 0!
//			System.out.println("PaIDs set finished for check duplicate-VW, total partioned reducer number : "+totRedNumForSearchTVector+", job.setNumReduceTasks(jobRedNum) should >= this value!!");
//			partitioner_forSearchTVector=new Partitioner_forSearchTVector<IntWritable>();
//			partitioner_forSearchTVector.setConf(conf);
//			// ***** setup finsihed ***//
//			processedQueryNum=0;
//			dispInter=5;
//			disp=false;
//			startTime=System.currentTimeMillis();
//			System.out.println("setup finsihed!");
//			super.setup(context);
//	 	}
//			
//		@Override
//		public void reduce(IntWritable QID, Iterable<DID_Score_Arr> docs_scores_I, Context context) throws IOException, InterruptedException {
//			/**
//			 * 1 reduce: for 1 query, merge mutiple vw-list into one list, each list should be ordered in ASC by docID! 
//			 */
//			
//			//key: query, value: vw and this vw-mathed docs and scores for this query
//
//			int thisQueryID=QID.get();
//			
//			//disp progress
//			processedQueryNum++;
//			if (processedQueryNum%dispInter==0){ 
//				disp=true;
//				System.out.println();
//			}
//			//********combine all vw_docs_scores for one query************
//			float[] mergResu=new float[comparePhotos.maxDocID+1]; //need to merge many times for every vw's DID_Score list, use DID as index in mergResu, so DID should start from 0!
//			int reducerNum=0;  long startTimeInRed=System.currentTimeMillis(); HashSet<Integer> checkDupliVWs=new HashSet<Integer>();
//			for(Iterator<DID_Score_Arr> it=docs_scores_I.iterator();it.hasNext();){
//				boolean debugShow=(disp && reducerNum%1000==0);
//				DID_Score_Arr oneInter=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//				//merge
//				if (debugShow) {//show example for one Query
//					System.out.println("\t this is "+reducerNum+"-th mergeSortedList_ASC(mergResu, Arr), this merge is for reducerNum:"+reducerNum+", before merge:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
//			    	System.out.println("\t --list size:"+oneInter.getArr().length+", top docs:"+General.selectArr(oneInter.getArr(), null, 5));
//			    	System.out.println("\t --mergResu top docs:"+General.floatArrToString(General.selectArrFloat(mergResu, null, 5), "_", "0.0000") );
//			    	System.out.println("\t --current memory info: "+General.memoryInfo());
//				}
//				General_IR.mergeSortedList_ASC(mergResu, oneInter.getArr());
//				if (debugShow) {//show example for one Query
//					System.out.println("\t "+reducerNum+"-th mergeSortedList_ASC(mergResu, Arr) finished: "+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
//			    	System.out.println("\t --mergResu top docs:"+General.floatArrToString(General.selectArrFloat(mergResu, null, 5), "_", "0.0000") );
//				}
//				reducerNum++;
//			}
//			if (disp){//show example for one Query
//				System.out.println("\t all mergeSortedList_ASC(mergResu, Arr) finished, from "+reducerNum+" reducers, time:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
//				System.out.println("\t --mergResu:"+General.floatArrToString(General.selectArrFloat(mergResu, null, 5), "_", "0.0000") );
//		    	System.out.println("\t --current memory info: "+General.memoryInfo());
//			}
//			//**************** find top ranked docs, and output them by vw, each vw has socred docs, and some of these are top ranked docs ****************
//			ArrayList<Integer> docs=new ArrayList<Integer>(mergResu.length); ArrayList<Float> scores=new ArrayList<Float>(mergResu.length); 
//			for (int docID = 0; docID < mergResu.length; docID++) {//docID is the index!
//				if (mergResu[docID]>0) {//this docID has match score
//					docs.add(docID);
//					scores.add(mergResu[docID]);
//				}
//			}
//			int scoredDocNum=docs.size();
//			//find top ranked docs
//			ArrayList<Integer> docs_top=new ArrayList<Integer>(maxIniRankLength*2); ArrayList<Float> scores_top=new ArrayList<Float>(maxIniRankLength*2); 
//			General_IR.rank_get_TopDocScores_PriorityQueue(docs, scores, maxIniRankLength, docs_top, scores_top, "DES", true);
////			//sort top ranked docs by docID in ASC!
////			Arrays.sort(docs_top.toArray(new Integer[0]));
////			General_IR.rank_get_AllSortedDocScores_ArraySort(null, scores, null, scores_top, null);
////			ArrayList<Integer> docRanks_sortbyDocID=new ArrayList<Integer>(docs_top.size()*2); ArrayList<Integer> sortedDocIDs=new ArrayList<Integer>(docs_top.size()*2); 
////			General_IR.rank_get_AllSortedDocIDs_treeSet(docRanks, docIDs, docRanks_sortbyDocID, sortedDocIDs, "ASC");
////			int[] topDocs_inSortedDocIDs=General.ArrListToIntArr(sortedDocIDs);
////			if (disp) {//for debug show
////				System.out.println(processedQueryNum+" querys finished! time:"+General.dispTime(System.currentTimeMillis()-startTime, "min")
////						+", current finished queryName: "+thisQueryID
////						+", total listed photos in its initial rank: "+scoredDocNum+", saved top doc numbers:"+docs_top.size()
////						+", top10 ranked Docs:"+docs_top.subList(0, 10)+", Scores: "+scores_top.subList(0, 10)
////						+", output is the topDocs with the order of docID in ASC, top-100 examples in sortedDocIDs: "+sortedDocIDs.subList(0, 100)
////						+", time:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
////		    	disp=false;
////			}
//			
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			System.out.println("\n one Reducer finished! total querys in this reducer:"+processedQueryNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
//			// ***** finsihed ***//			
//			super.setup(context);
//	 	}
//	}
//
//	
//	
//	
//	
//	
//	//******** job2_2 **************	
//	public static class Reducer_SearchTVector_getHMScore extends Reducer<IntWritable,IntArr_HESig_ShortArr_Arr,IntWritable,DID_Score_Arr>  {
//
//		private Configuration conf;
//		private FileSystem HDFS;
//		private ComparePhotos_LocalFeature comparePhotos;
//		private float[] docScores;
//		private int VWFileInter;
//		private String TVectorPath;
//		private int[] PaIDs;
//		private StringBuffer vws_queryNums;
//		private int reduceNum;
//		private int dispInter_reduce;
//		private boolean disp;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			conf = context.getConfiguration();
//			HDFS=FileSystem.get(conf);
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("PaIDs.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//***** set TVector SeqFile Path***//
//			VWFileInter=Integer.valueOf(conf.get("mapred.VWFileInter"));
//			TVectorPath = "hdfs://p-head03.alley.sara.nl/user/yliu/"+conf.get("mapred.TVectorPath");
//			System.out.println("TVectorPath:"+TVectorPath+", VWFileInter:"+VWFileInter);
//			//********** setup ComparePhotos_LocalFeature ***************
//			comparePhotos=new ComparePhotos_LocalFeature();
//			System.out.println("current memory:"+General.memoryInfo());
//			//-setup_scoreDoc
//			String info_setup_scoreDoc= comparePhotos.setup_scoreDoc(Integer.valueOf(conf.get("mapred.HMDistThr_selDoc")), Double.valueOf(conf.get("mapred.HMWeight_deta")), 
//					-1, "docInfo.file", "TVectorInfo.file", conf);
//			System.out.println(info_setup_scoreDoc);
//			System.out.println("current memory:"+General.memoryInfo());
//			//********** setup merge doc scores ***************
//			docScores=new float[comparePhotos.maxDocID+1];//merge doc socre from mutiple vws
//			System.out.println("docScores initialization finished! current memory:"+General.memoryInfo());
//			//** set PaIDs **//
//			PaIDs= (int[]) General.readObject("PaIDs.file");
//			System.out.println("PaIDs load finished, total partioned reducer number : "+General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs)+", job.setNumReduceTasks(jobRedNum) should == this value!!");
//			// ***** setup finsihed ***//
//			System.out.println("setup finsihed!");
//			vws_queryNums=new StringBuffer();
//			reduceNum=0;
//			dispInter_reduce=1;
//			disp=false;
//			super.setup(context);
//	 	}
//		
//		@Override
//		public void reduce(IntWritable VW, Iterable<IntArr_HESig_ShortArr_Arr> QueryNames_feats, Context context) throws IOException, InterruptedException {
//			//QueryNameSigs: QueryName-Integer, Sigs:-ByteArrList
//
//			int progInter=1000;  int vw=VW.get();
//
//			reduceNum++;
//			if (reduceNum%dispInter_reduce==0) {
//				disp=true;
//			}
//			
//			General.dispInfo_ifNeed(disp, "\n this reduce is for VW: "+VW+", total allocated reducers for this vw: "+General_Hadoop.getVWReducerNum_from_vwPartitionIDs(PaIDs, vw));
//			
//			long startTime=System.currentTimeMillis();
//			int queryNum_thisVW=0; int queryFeatNum_thisVW=0; HESig[] queryFeats; 
//			
//			//********read TVector(SeqFile) into memory************
//			String TVPath=General_Hadoop.getOneVWFilePath(TVectorPath, vw, VWFileInter);//1vw, 1 seqFile
//			if (HDFS.exists(new Path(TVPath))) {
//				General.dispInfo_ifNeed(disp, "TVPath exists: "+ TVPath+", before read TVector, current memory info: "+General.memoryInfo());
//				ArrayList<Integer> TVector_docIDs=new ArrayList<Integer>(1000*1000); ArrayList<SURFfeat[]> TVector_feats=new ArrayList<SURFfeat[]>(1000*1000);
//				int TVectorFeatNum=General_BoofCV.readTVectorIntoMemory(HDFS, TVPath, conf, vw, TVector_docIDs, TVector_feats);
//				General.dispInfo_ifNeed(disp, "read this TVector into memory finished! docNum: "+ TVector_docIDs.size()+", featNum: "+TVectorFeatNum
//						+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s")+", current memory info: "+General.memoryInfo());
//				//******* search TVector ***********
//				HashSet<Integer> checkDupliQuerys=new HashSet<Integer>();
//				startTime=System.currentTimeMillis();
//				//process querys have this vw
//	        	for(Iterator<IntArr_HESig_ShortArr_Arr> it=QueryNames_feats.iterator();it.hasNext();){
//	        		IntArr_HESig_ShortArr_Arr Querys=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//	        		int queryNum_thisInterator=Querys.getIntegers().length;
//	        		for(int query_i=0;query_i<queryNum_thisInterator;query_i++){
//		        		//********* process one query *********
//						int queryName=Querys.getIntegers()[query_i]; queryFeats=Querys.getFeats()[query_i].getArr();
//						General.Assert(checkDupliQuerys.add(queryName), "err! duplicate querys for VW:"+vw+", duplicate query:"+queryName);
//						//compare docs in TVector for this query
//						for(int doc_i=0;doc_i<TVector_docIDs.size();doc_i++){
//							//get match link and score
//							float hmScore=General_BoofCV.compare_HESigs(queryFeats, TVector_feats.get(doc_i), comparePhotos.HMDistThr, comparePhotos.hammingW);
//							if (hmScore>0) {
//								docScores[TVector_docIDs.get(doc_i)]+=hmScore*comparePhotos.idf_squre[vw];
//							}
//						}
//						//************ report progress ******************
//	        			queryNum_thisVW++; queryFeatNum_thisVW+=queryFeats.length;
//						if(disp==true && queryNum_thisVW%progInter==0){
//							long time=System.currentTimeMillis()-startTime;
//							System.out.println("\t --curent total finished querys:"+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW
//									+", time:"+General.dispTime(time, "min")+", average compare time per sig pair:"+(double)time/TVectorFeatNum/queryFeatNum_thisVW);
//							System.out.println("\t ----current memory info: "+General.memoryInfo());
//						}
//	        		}
//		        }
//			}else {
//				System.out.println("\n -Warning!!  for VW-"+vw+", query exist this vw, but no photo in dataset exist this vw, TVector not exist for this vw!");
//			}
//			
//        	
//        	//*** some statistic ********
//        	General.dispInfo_ifNeed(disp, "one reduce finished! total query number for this vw: "+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW);
//        	disp=false;
//        	
//        	vws_queryNums.append(vw+"_"+queryNum_thisVW+"_"+queryFeatNum_thisVW+",");
//        	
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("\n Reducer finished! reduceNum(vws num): "+reduceNum+", vws_queryNums_queryFeatNums: "+vws_queryNums.toString());
//			//out put docScores
//			
//			super.setup(context);
//	 	}
//	}
//
//	//******** job2_3 **************	
//	public static class Reducer_buildInitialRank_HE extends Reducer<IntWritable,VW_DID_Score_Arr_Arr,IntWritable,IntArr>  {
//			Configuration conf;
//			private ComparePhotos_LocalFeature comparePhotos;
//			
//			private int maxIniRankLength;
//			
//			private Path tempPathFor_VWDocs; //save vw_docs to seqFile on local node
//			
//			private int[] PaIDs;
//			private int totRedNumForSearchTVector;
//			private Partitioner_forSearchTVector<IntWritable> partitioner_forSearchTVector;
//			
//			private int processedQueryNum;
//			private long startTime;
//			private int dispInter;
//			private boolean disp;
//		
//			@Override
//			protected void setup(Context context) throws IOException, InterruptedException {
//				conf = context.getConfiguration();	
//				//setup ComparePhotos_LocalFeature
//				comparePhotos=new ComparePhotos_LocalFeature();
//				System.out.println("current memory:"+General.memoryInfo());
//				//-setup_scoreDoc
//				String info_setup_scoreDoc= comparePhotos.setup_scoreDoc(Integer.valueOf(conf.get("mapred.HMDistThr_selDoc")), Double.valueOf(conf.get("mapred.HMWeight_deta")), 
//						-1, "docInfo.file", null, conf);
//				System.out.println(info_setup_scoreDoc);
//				System.out.println("current memory:"+General.memoryInfo());
//				//***** set maxIniRankLength for further rerank ***//
//				maxIniRankLength=Integer.valueOf(conf.get("mapred.maxIniRankLength")); //select top rank to do 1vs1 and HPM check
//				System.out.println("select top-"+maxIniRankLength+" in the initial rankList to do further rerank: 1vs1 and HPM check!");
//				//***** set tempPathFor_VWDocs for save vw_docs to SeqFile ***//
//				tempPathFor_VWDocs= new Path("vw_docs.seq");
//				String[] info=General_Hadoop.checkFileInNode("vw_docs.seq");//absoultPath, parentPath, files in parentPath
//				System.out.println("tempPathFor_VWDocs for save vw_docs setted: "+tempPathFor_VWDocs+", its absolut path in local-node: \n"+info[0]);
//				System.out.println("files in its parent path: \n"+info[2]);
//				// ***** setup PaID for check duplicate-VW ***//
//				PaIDs= (int[]) General.readObject("PaIDs.file");//each element in PaIDs is mutipled by 10!
//				totRedNumForSearchTVector=General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs); //reducer number for seachTVector, PaIDs: values from 0!
//				System.out.println("PaIDs set finished for check duplicate-VW, total partioned reducer number : "+totRedNumForSearchTVector+", job.setNumReduceTasks(jobRedNum) should >= this value!!");
//				partitioner_forSearchTVector=new Partitioner_forSearchTVector<IntWritable>();
//				partitioner_forSearchTVector.setConf(conf);
//				// ***** setup finsihed ***//
//				processedQueryNum=0;
//				dispInter=5;
//				disp=false;
//				startTime=System.currentTimeMillis();
//				System.out.println("setup finsihed!");
//				super.setup(context);
//		 	}
//				
//			@Override
//			public void reduce(IntWritable QID, Iterable<VW_DID_Score_Arr_Arr> docs_scores_I, Context context) throws IOException, InterruptedException {
//				/**
//				 * 1 reduce: for 1 query, merge mutiple vw-list into one list, each list should be ordered in ASC by docID! 
//				 */
//				
//				//key: query, value: vw and this vw-mathed docs and scores for this query
//
//				int thisQueryID=QID.get();
//				
//				//disp progress
//				processedQueryNum++;
//				if (processedQueryNum%dispInter==0){ 
//					disp=true;
//					System.out.println();
//				}
//				//********combine all vw_docs_scores for one query************
//				float[] mergResu=new float[comparePhotos.maxDocID+1]; //need to merge many times for every vw's DID_Score list, use DID as index in mergResu, so DID should start from 0!
//				IntWritable local_key=new IntWritable(); IntArr local_value=new IntArr();
//				SequenceFile.Writer seqFileWr_vw_Docs=new SequenceFile.Writer(FileSystem.getLocal(conf), conf, tempPathFor_VWDocs, local_key.getClass(),local_value.getClass());
//				int vwNum=0;  long startTimeInRed=System.currentTimeMillis(); HashSet<Integer> checkDupliVWs=new HashSet<Integer>();
//				for(Iterator<VW_DID_Score_Arr_Arr> it=docs_scores_I.iterator();it.hasNext();){
//					VW_DID_Score_Arr_Arr oneInter=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//					for (VW_DID_Score_Arr one: oneInter.getArr()) {
//						vwNum++;
//						boolean debugShow=(disp && vwNum%2000==0);
//						//add vw
//						int this_vw=one.vw;
//						//save to local seqFile, each DID_Score_Arr should be ordered in ASC by docID! this is guanteed by: ASC docID in TVector
//						local_key.set(this_vw);
//						seqFileWr_vw_Docs.append(local_key, new IntArr(one.getDocs()));
//						//merge
//						if (debugShow) {//show example for one Query
//							System.out.println("\t this is "+vwNum+"-th mergeSortedList_ASC(mergResu, Arr), this merge is for vw:"+this_vw+", before merge:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
//					    	System.out.println("\t --list size:"+one.docs_scores.getArr().length+", top docs:"+General.selectArr(one.docs_scores.getArr(), null, 5));
//					    	System.out.println("\t --mergResu top docs:"+General.floatArrToString(General.selectArrFloat(mergResu, null, 5), "_", "0.0000") );
//					    	System.out.println("\t --current memory info: "+General.memoryInfo());
//						}
//						General_IR.mergeSortedList_ASC(mergResu, one.docs_scores.getArr());
//						if (debugShow) {//show example for one Query
//							System.out.println("\t "+vwNum+"-th mergeSortedList_ASC(mergResu, Arr) finished: "+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
//					    	System.out.println("\t --mergResu top docs:"+General.floatArrToString(General.selectArrFloat(mergResu, null, 5), "_", "0.0000") );
//						}
//						General.Assert(checkDupliVWs.add(this_vw), "err! duplicate VWs for query:"+thisQueryID+", duplicate vw:"+this_vw
//								+", its allocated reducer num when searchTVector:"+General_Hadoop.getVWReducerNum_from_vwPartitionIDs(PaIDs, this_vw)
//								+", partitionID:"+partitioner_forSearchTVector.getPartition(new IntWritable(this_vw), QID, totRedNumForSearchTVector));
//					}
//				}
//				seqFileWr_vw_Docs.close();
//				if (disp){//show example for one Query
//					System.out.println("\t all mergeSortedList_ASC(mergResu, Arr) finished, vwNum: "+vwNum+", time:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
//					System.out.println("\t --mergResu:"+General.floatArrToString(General.selectArrFloat(mergResu, null, 5), "_", "0.0000") );
//			    	System.out.println("\t --current memory info: "+General.memoryInfo());
//				}
//				
//				//**************** find top ranked docs, and output them by vw, each vw has socred docs, and some of these are top ranked docs ****************
//				ArrayList<Integer> docs=new ArrayList<Integer>(mergResu.length); ArrayList<Float> scores=new ArrayList<Float>(mergResu.length); 
//				for (int docID = 0; docID < mergResu.length; docID++) {//docID is the index!
//					if (mergResu[docID]>0) {//this docID has match score
//						docs.add(docID);
//						scores.add(mergResu[docID]);
//					}
//				}
//				int scoredDocNum=docs.size();
//				//find top ranked docs
//				ArrayList<Integer> docs_top=new ArrayList<Integer>(maxIniRankLength*2); ArrayList<Float> scores_top=new ArrayList<Float>(maxIniRankLength*2); 
//				General_IR.rank_get_TopDocScores_PriorityQueue(docs, scores, maxIniRankLength, docs_top, scores_top, "DES", true);
//				//sort top ranked docs by docID in ASC!
//				ArrayList<Integer> docRanks=new ArrayList<Integer>(docs_top.size()*2); ArrayList<Integer> docIDs=new ArrayList<Integer>(docs_top.size()*2); 
//				for (int i = 0; i < docs_top.size(); i++) {
//					docRanks.add(i);
//					docIDs.add(docs_top.get(i));//use docID as ranking score
//				}
//				ArrayList<Integer> docRanks_sortbyDocID=new ArrayList<Integer>(docs_top.size()*2); ArrayList<Integer> sortedDocIDs=new ArrayList<Integer>(docs_top.size()*2); 
//				General_IR.rank_get_AllSortedDocIDs_treeSet(docRanks, docIDs, docRanks_sortbyDocID, sortedDocIDs, "ASC");
//				int[] topDocs_inSortedDocIDs=General.ArrListToIntArr(sortedDocIDs);
//				if (disp) {//for debug show
//					System.out.println("\t selected top docs:"+sortedDocIDs.size()+", top-100 examples in sortedDocIDs: "+sortedDocIDs.subList(0, 100)
//							+", time:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
//				}
//				//find topDocs' involved vws
//				SequenceFile.Reader seqFileRe_vw_Docs=new SequenceFile.Reader(FileSystem.getLocal(conf), tempPathFor_VWDocs, conf);
//				vwNum=0; int commonNumTot=0; 
//				while (seqFileRe_vw_Docs.next(local_key, local_value)) {
//					vwNum++;
//					boolean debugShow=(disp && vwNum%2000==0);
//					int thisVW=local_key.get();
//					int[] thisDocs=local_value.getIntArr();//must be ordered based on docID in ASC!
//					ArrayList<int[]> commons=General.findCommonElementInds_twoSorted_ASC_loopShotArr(topDocs_inSortedDocIDs, thisDocs);//find common elements in two sorted arr
////					HashSet<Integer> checkDuplicate=new HashSet<Integer>();
//					for (int[] one : commons) {
//						context.write(new IntWritable(thisVW), new IntArr(new int[]{thisQueryID, one[0], docRanks_sortbyDocID.get(one[1])})); //key_vw, value_QID_DID_rank
////						General.Assert(checkDuplicate.add(one[0]), "err! duplicate docs in commons! vw:"+thisVW+", query:"+thisQueryID
////								+", duplicate doc:"+one[0]+", rank in topDocs:"+one[1]);
//					}
////					General.Assert(checkDuplicate.size()==commons.size(), "err! duplicate docs in commons! checkDuplicate.size():"+checkDuplicate.size()+", commons.size():"+commons.size());
//					commonNumTot+=commons.size();
//					//debug show common elements
//					if (debugShow) {//for debug show
//						System.out.println("\t ---"+vwNum+"-th vw(finding common docs), time:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms")+", thisVW: "+thisVW+", top-100 in thisDocs: "+General.IntArrToString(General.selectArrInt(thisDocs, null, 100),"_"));
//						System.out.println("\t ---this vw:"+thisVW+", commonDocs: "+commons.size());
//					}
//				}
//				//clean-up seqFile in local node
//				seqFileRe_vw_Docs.close();
//				General.Assert(FileSystem.getLocal(conf).delete(tempPathFor_VWDocs,true), "err in delete tempPathFor_VWDocs, not successed!") ;
//
//				if (disp){//show example for one Query
//					System.out.println(processedQueryNum+" querys finished! time:"+General.dispTime(System.currentTimeMillis()-startTime, "min")
//							+", current finished queryName: "+thisQueryID
//							+", matched vwNum:"+vwNum+", total listed photos in its initial rank: "+scoredDocNum+", saved top doc numbers:"+docs_top.size()
//							+", top10 ranked Docs:"+docs_top.subList(0, 10)+", Scores: "+scores_top.subList(0, 10));
//					System.out.println("total common docs between topDocs and scoredDocs in all vws':"+commonNumTot+", on average, "+(float)commonNumTot/vwNum+" common docs per vw");
//			    	disp=false;
//				}
//			}
//			
//			@Override
//			protected void cleanup(Context context) throws IOException, InterruptedException {
//				System.out.println("\n one Reducer finished! total querys in this reducer:"+processedQueryNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
//				// ***** finsihed ***//			
//				super.setup(context);
//		 	}
//		}
//
//	//******** job3 **************
//	public static class Mapper_selectTopRankDocs extends Mapper<IntWritable,IntArr,IntWritable,IntArr>{
//		
//		private int reRankByHPM;
//		private int procSamples;
//		private int procSelSamples;
//		private int dispInter;
//		private long startTime, endTime;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf = context.getConfiguration();
//			//***** set reRankByHPM for 1vs1&HPM rerank ***//
//			reRankByHPM=Integer.valueOf(conf.get("mapred.reRankByHPM")); //select top rank to do 1vs1 and HPM check
//			System.out.println("select top-"+reRankByHPM+" in the initial rankList to do 1vs1 and HPM check!");
//			//set procSamples
//			procSamples=0;
//			procSelSamples=0;
//			//set dispInter
//			dispInter=5000;
//			startTime=System.currentTimeMillis(); //startTime
//			
//			System.out.println("mapper setup finsihed!");
//			super.setup(context);
//	 	}
//		
//		@Override
//		protected void map(IntWritable key, IntArr value, Context context) throws IOException, InterruptedException {
//			//key: vw
//			//value: QID_DID_Rank
//			procSamples++;
//			if(value.getIntArr()[2]<reRankByHPM){//doc's rank is within the rerank scale
//				procSelSamples++;
//				context.write(key, value);
//			}
//			//disp
//			if((procSamples)%dispInter==0){ 							
//				endTime=System.currentTimeMillis(); //end time 
//				System.out.println( "select Samples, "+procSamples+" Samples finished!! selected: "+procSelSamples+" ......"+ General.dispTime (endTime-startTime, "min"));
//			}
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//		    endTime=System.currentTimeMillis(); //end time 
//			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples+", selected: "+procSelSamples+" ....."+ General.dispTime ( endTime-startTime, "min"));
//			super.setup(context);
//	 	}
//	}
//
//	public static class Reducer_groupVW_QID_DocIDs extends Reducer<IntWritable,IntArr,IntWritable,IntArr>  {
//		private Configuration conf;
//		private FileSystem hdfs;
//		private int VWFileInter;
//		private String vw_iniDocsPath;
//		private int reduceNum;
//		private StringBuffer vws;
//		private int dispInter;
//
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			String homePath="hdfs://p-head03.alley.sara.nl/user/yliu/";
//			conf=context.getConfiguration();
//			hdfs=FileSystem.get(conf);
//			VWFileInter=Integer.valueOf(conf.get("mapred.VWFileInter"));
//			vw_iniDocsPath=homePath+conf.get("mapred.vw_iniDocsPath");
//			System.out.println("vw_iniDocsPath:"+vw_iniDocsPath+", VWFileInter:"+VWFileInter);
//			// ***** setup finsihed ***//
//			reduceNum=0;
//			vws=new StringBuffer();
//			dispInter=50;
//			System.out.println("setup finsihed!");
//			super.setup(context);
//	 	}
//		
//		@Override
//		public void reduce(IntWritable VW, Iterable<IntArr> QID_DIDs, Context context) throws IOException, InterruptedException {
//			//key: VW, value: QID_DID_Rank
//
//			int vw=VW.get();
//			//******** matched  ************	
//			HashMap<Integer, ArrayList<Integer>> Q_Docs=new HashMap<Integer, ArrayList<Integer>>(); 
//			for(Iterator<IntArr> it=QID_DIDs.iterator();it.hasNext();){// loop over all HashMaps				
//				IntArr oneQ_D_R=it.next();
//				int thisQ=oneQ_D_R.getIntArr()[0]; int thisD=oneQ_D_R.getIntArr()[1]; 
//				ArrayList<Integer> thisQueryDocs=Q_Docs.get(thisQ);
//				if (thisQueryDocs==null) {
//					thisQueryDocs=new ArrayList<Integer>();
//					thisQueryDocs.add(thisD);
//					Q_Docs.put(thisQ, thisQueryDocs);
//				}else {
//					thisQueryDocs.add(thisD);
//				}
//			}		
//			//outPut the matchedDocs into seq on vw_iniDocsPath
//			SequenceFile.Writer VWFile_Writer=new SequenceFile.Writer(hdfs, conf, new Path(General_Hadoop.getOneVWFilePath(vw_iniDocsPath, vw, VWFileInter)), IntWritable.class, IntArr.class);
//			//--1st elemetn in the output is to mark vw!
//			VWFile_Writer.append(new IntWritable(vw), new IntArr(new int[]{vw,vw,vw})); //key_vw, value_[vw,vw,vw]
//			//--output query_docs
//			int queryNum=0; int docNum=0;
//			for (Entry<Integer, ArrayList<Integer>> oneQ_Docs : Q_Docs.entrySet()) {
//				HashSet<Integer> checkDuplicate=new HashSet<Integer>(oneQ_Docs.getValue());
//				General.Assert(checkDuplicate.size()==oneQ_Docs.getValue().size(), "err, duplicate docs in VW:"+vw+", Q:"+oneQ_Docs.getKey()+", its docs:"+oneQ_Docs.getValue());
//				int[] docs=General.ArrListToIntArr(oneQ_Docs.getValue());
//				Arrays.sort(docs);//sort docs in asc order
//				VWFile_Writer.append(new IntWritable(oneQ_Docs.getKey()), new IntArr(docs)); //key_queryID, value_matchedDocs
//				queryNum++;
//				docNum+=oneQ_Docs.getValue().size();
//			}
//			VWFile_Writer.close();
//				
//			//disp progress
//			reduceNum++;
//			if (reduceNum%dispInter==0){ 
//				System.out.println(reduceNum+" reduce(vw) finished! current finished vw: "+vw+", total queryNum: "+queryNum+", matched photos:"+docNum);
//			}
//			vws.append(vw+"_"+queryNum+"_"+docNum+", ");
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("this reducer is for vw_matchedDocs, one reducer finished! total "+reduceNum+" vws in this Reducer, vw_queryNum_docNum: "+vws.toString());
//			super.setup(context);
//	 	}
//	}
//	
//	//******** job4 **************	
//	public static class Reducer_SearchTVector_getDocFeat extends Reducer<IntWritable,IntArr_HESig_ShortArr_Arr,Key_QID_VW,Int_MatchFeat_Arr>  {
//
//		private Configuration conf;
//		private FileSystem HDFS;
//		private int VWFileInter;
//		private String TVectorPath;
//		private String vw_iniDocsPath;
//		private int HMDistThr;
//		private int[] PaIDs;
//		private StringBuffer vws_queryNums;
//		private int reduceNum;
//		private int dispInter_reduce;
//		private boolean disp;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			conf = context.getConfiguration();
//			HDFS=FileSystem.get(conf);
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("PaIDs.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//***** set TVector SeqFiles Path***//
//			VWFileInter=Integer.valueOf(conf.get("mapred.VWFileInter"));
//			TVectorPath = "hdfs://p-head03.alley.sara.nl/user/yliu/"+conf.get("mapred.TVectorPath");
//			System.out.println("TVectorPath:"+TVectorPath+", VWFileInter:"+VWFileInter);
//			//***** set vw_matchedDocs MapFile Path***//
//			vw_iniDocsPath = conf.get("mapred.vw_iniDocsPath");
//			System.out.println("vw_iniDocsPath: "+vw_iniDocsPath);
//			//********* set Hamming distance threshold***//
//			HMDistThr=Integer.valueOf(conf.get("mapred.HMDistThr_rankDoc"));
//			System.out.println("HMDistThr: "+HMDistThr);
//			//********** set PaIDs **************//
//			PaIDs= (int[]) General.readObject("PaIDs.file");
//			System.out.println("PaIDs load finished, total partioned reducer number : "+(PaIDs[PaIDs.length-1]+1)+", job.setNumReduceTasks(jobRedNum) should == this value!!");
//			// ***** setup finsihed ***//
//			System.out.println("setup finsihed!");
//			vws_queryNums=new StringBuffer();
//			reduceNum=0;
//			dispInter_reduce=1;
//			disp=false;
//			super.setup(context);
//	 	}
//		
//		@Override
//		public void reduce(IntWritable VW, Iterable<IntArr_HESig_ShortArr_Arr> QueryNames_feats, Context context) throws IOException, InterruptedException {
//			//QueryNameSigs: QueryName-Integer, Sigs:-ByteArrList
//
//			int progInter=500;  int vw=VW.get();
//			
//			reduceNum++;
//			if (reduceNum%dispInter_reduce==0) {
//				disp=true;
//			}
//			
//			General.dispInfo_ifNeed(disp,"\n this reduce is for VW: "+VW+", total allocated reducers for this vw: "+General_Hadoop.getVWReducerNum_from_vwPartitionIDs(PaIDs, vw));	
//			
//			//******** set output key,value ************
//			Key_QID_VW redOutKey=new Key_QID_VW();
//			Int_MatchFeat_Arr redOutValue =new Int_MatchFeat_Arr();
//			
//			long startTime=System.currentTimeMillis();
//
//			//******** read vw_matchedDocs into memory ************
//			String one_vw_iniDocsPath=General_Hadoop.getOneVWFilePath(vw_iniDocsPath, vw, VWFileInter);//1vw, 1 seqFile
//			General.dispInfo_ifNeed(disp,"\t one_vw_iniDocsPath: "+ one_vw_iniDocsPath);
//			int queryNum_thisVW=0; int queryFeatNum_thisVW=0; int queryNum_existMatch=0; 
//			if (HDFS.exists(new Path(one_vw_iniDocsPath))) {
//				HashMap<Integer, int[]> QID_DIDs = new HashMap<Integer, int[]>();
//				int allQueryMatchedDocNum=General_BoofCV.readVW_MatchDocsIntoMemory(HDFS, one_vw_iniDocsPath, conf, vw, QID_DIDs);
//				General.dispInfo_ifNeed(disp,"\t read this vw_matchedDocs into memory finished! queryNum: "+ QID_DIDs.size()+", allQueryMatchedDocNum: "+allQueryMatchedDocNum
//						+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s")+", current memory info: "+General.memoryInfo());
//				//******** read TVector(SeqFile) into memory************
//				String TVPath=General_Hadoop.getOneVWFilePath(TVectorPath, vw, VWFileInter);//1vw, 1 seqFile
//				General.dispInfo_ifNeed(disp,"\t TVPath: "+ TVPath+", before read TVector, current memory info: "+General.memoryInfo());
//				ArrayList<Integer> TVector_docIDs=new ArrayList<Integer>(); ArrayList<SURFfeat[]> TVector_feats=new ArrayList<SURFfeat[]>();
//				int TVectorFeatNum=General_BoofCV.readTVectorIntoMemory(HDFS, TVPath, conf, vw, TVector_docIDs, TVector_feats);
//				General.dispInfo_ifNeed(disp,"\t read this TVector into memory finished! docNum: "+ TVector_docIDs.size()+", featNum: "+TVectorFeatNum
//						+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s")+", current memory info: "+General.memoryInfo());
//				//******* search TVector ***********
//				HESig[] queryFeats; int[] matchedDocs=null; startTime=System.currentTimeMillis();
//				//process querys have this vw
//	        	for(Iterator<IntArr_HESig_ShortArr_Arr> it=QueryNames_feats.iterator();it.hasNext();){
//	        		IntArr_HESig_ShortArr_Arr Querys=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//	        		int queryNum_thisInterator=Querys.getIntegers().length; boolean showThisQuery=false;
//	        		for(int query_i=0;query_i<queryNum_thisInterator;query_i++){
//		        		//********* process one query *********
//						int queryName=Querys.getIntegers()[query_i]; queryFeats=Querys.getFeats()[query_i].getArr(); 	 
//						queryNum_thisVW++; queryFeatNum_thisVW+=queryFeats.length;
//						if(disp && queryNum_thisVW%progInter==0){
//							showThisQuery=true;
//						}
//						int matchDocNum_right=0; int matchFeatNum=0;
//						if((matchedDocs=QID_DIDs.get(queryName))!=null){//this vw exist top-selected docs for this query
//							redOutKey.set(queryName, vw);
//							//get selected docs' match feat in TVector for this query
//							matchDocNum_right=matchedDocs.length; boolean showOneExample=showThisQuery;
//							ArrayList<int[]> commonDocs=General.findCommonElementInds_twoSorted_ASC_loopShotArr(matchedDocs, General.ArrListToIntArr(TVector_docIDs));
//							if (commonDocs==null) {
//								throw new InterruptedException("err!! no common docs! should matchedDocs:"+matchedDocs.length+", "+matchedDocs[0]+"_"+matchedDocs[matchedDocs.length-1]
//										+", TVector_docIDs:"+TVector_docIDs.size()+", "+TVector_docIDs.get(0)+"_"+TVector_docIDs.get(TVector_docIDs.size()-1));
//							}else {
//								General.Assert(commonDocs.size()==matchDocNum_right, "err in Reducer_SearchTVector_getDocFeat, for queryName:"+queryName
//										+", matchDocNum_act:"+commonDocs.size()+", should == matchDocNum_right:"+matchDocNum_right+", docs should match:"+General.IntArrToString(matchedDocs, "_"));
//							}
//							for (int[] oneDoc : commonDocs) {
//								int docInd_inTV=oneDoc[2];
//								//get match link and score
//								MatchFeat_Arr oneDocMatches= General_BoofCV.compare_HESigs(queryFeats, TVector_feats.get(docInd_inTV), HMDistThr);
//								matchFeatNum+=oneDocMatches.getArr().length;
//								//outputfile: SequcenceFile; outputFormat: key->Key_QID_VW  value->Int_MatchFeat_ShortArr
//								redOutValue.set(TVector_docIDs.get(docInd_inTV), oneDocMatches);
//								context.write(redOutKey, redOutValue); 	       
//								if(showOneExample){//for debug
//									System.out.println("\t ---show one example: 1 doc's matches of 1 query --- queryName:"+queryName+", docID:"+TVector_docIDs.get(docInd_inTV)+", vw:"+vw+", its Matches:"+oneDocMatches.getArr().length);
//									for (int m_i = 0;m_i < oneDocMatches.getArr().length; m_i++) {
//										System.out.println("\t ------ match-"+m_i+": "+oneDocMatches.getArr()[m_i].toString());
//									}
//									showOneExample=false;
//								}
//							}
//							queryNum_existMatch++;
//						}
//						//************ report progress ******************
//						if(showThisQuery){
//							System.out.println("\t -curent total finished querys:"+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW+", queryNum_existMatch:"+queryNum_existMatch
//									+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//							System.out.println("\t ---show info for current query: "+queryName+", sig number:"+queryFeats.length);
//							System.out.println("\t ---selected matched docs number: "+matchDocNum_right+", tot matchFeatNum:"+matchFeatNum);
//							System.out.println("\t ---current memory info: "+General.memoryInfo());
//							showThisQuery=false;
//						}
//	        		}
//		        }
//			}else {
//				General.dispInfo_ifNeed(disp,"this vw do not have matched query-doc! just ignor!");
//			}
//        	//*** some statistic ********
//        	General.dispInfo_ifNeed(disp,"\t one reduce finished! total query number for this vw: "+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW+", queryNum_existMatch:"+queryNum_existMatch);
//        	vws_queryNums.append(vw+"_"+queryNum_thisVW+"_"+queryFeatNum_thisVW+"_"+queryNum_existMatch+",");
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("Reducer finished! reduceNum: "+reduceNum+", vws_queryNums_queryFeatNums_queryNumExistMatch: "+vws_queryNums.toString());
//			super.setup(context);
//	 	}
//	}
//
//	//******** job5 **************	
//	public static class Mapper_transfer_finalRank extends Mapper<Key_QID_VW,Int_MatchFeat_Arr, Key_QID_DID, Int_MatchFeat_Arr>{
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
//	public static class Reducer_buildRank_final_saveScore extends Reducer<Key_QID_DID,Int_MatchFeat_Arr,IntWritable,IntList_FloatList>  {
//		
//		private int topRank;
//		private int processedQueryNum;
//		private int reduceNum;
//		private long startTime;
//		private int dispInter;
//		private String rerankFlag;
//		private int reRankByHPM;
//		
//		private MapFile.Reader queryFeatReader;
//		
//		private ComparePhotos_LocalFeature comparePhotos;
//		
//		private ArrayList<Integer> docIDs;
//		private ArrayList<Float> docScores;
//		private int queryID_previous;
//		private boolean disp;
//		
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf = context.getConfiguration();
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("docInfo.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//***** select top rank for output ***//
//			topRank=Integer.valueOf(conf.get("mapred.topRank")); //select top rank as output
//			System.out.println("select top-"+topRank+" in rankList as output");
//			//***** set rerankFlag ***********
//			rerankFlag=conf.get("mapred.rerankFlag");
//			System.out.println("rerankFlag:"+rerankFlag);
//			//***** set reRankByHPM for 1vs1&HPM rerank ***//
//			reRankByHPM=Integer.valueOf(conf.get("mapred.reRankByHPM")); //select top rank to do 1vs1 and HPM check
//			System.out.println("select top-"+reRankByHPM+" in the initial rankList to do 1vs1 and HPM check!");
//			//***** read query feats ***//
//			queryFeatReader=General_Hadoop.openMapFileInNode("data", conf, true);
//			System.out.println("open query-Feat-MapFile finished");
//			//********** setup ComparePhotos_LocalFeature ***************
//			comparePhotos=new ComparePhotos_LocalFeature();
//			System.out.println("current memory:"+General.memoryInfo());
//			//-setup_scoreDoc
//			String info_setup_scoreDoc= comparePhotos.setup_scoreDoc(Integer.valueOf(conf.get("mapred.HMDistThr_rankDoc")), Double.valueOf(conf.get("mapred.HMWeight_deta")), 
//					Integer.valueOf(conf.get("mapred.HPM_level")), "docInfo.file", "TVectorInfo.file", conf);
//			System.out.println(info_setup_scoreDoc);
//			System.out.println("current memory:"+General.memoryInfo());
//			//***** set queryID_previous ***//
//			docIDs=new ArrayList<Integer>(reRankByHPM*2); //save all docs' score for one query
//			docScores=new ArrayList<Float>(reRankByHPM*2); 
//			queryID_previous=-1;
//			// ***** setup finsihed ***//
//			processedQueryNum=0;
//			reduceNum=0;
//			startTime=System.currentTimeMillis();
//			dispInter=10;
//			disp=true;
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
//				ArrayList<Integer> topDocs=new ArrayList<Integer>(topRank); ArrayList<Float> topScores=new ArrayList<Float>();
//				General_IR.rank_get_TopDocScores_PriorityQueue(docIDs, docScores, topRank, topDocs, topScores, "DES", true);
//				disp=false;
//				context.write(new IntWritable(queryID_previous), new IntList_FloatList(topDocs,topScores));
//				//disp progress
//				processedQueryNum++;
//				if (processedQueryNum%dispInter==0){ 
//					System.out.println("\n"+processedQueryNum+" querys finished! time:"+General.dispTime(System.currentTimeMillis()-startTime, "min")
//							+", current finished queryName: "+queryID_previous+", total selected photos in its initial rank: "+docIDs.size()+", saved top doc numbers:"+topDocs.size()
//							+", top10Docs:"+topDocs.subList(0, Math.min(10, topDocs.size()))+", top10Scores:"+topScores.subList(0, Math.min(10, topDocs.size())));
//					disp=true;
//				}
//				//prepareForNext
//				docIDs.clear(); docScores.clear();
//			}
//
//			//********combine all vw_MatchFeats for one doc************
//			int docID=QID_DID.docID;
//			ArrayList<Int_MatchFeat_Arr>  matches = new ArrayList<Int_MatchFeat_Arr>(); 
//			for(Iterator<Int_MatchFeat_Arr> it=vw_MatchFeats.iterator();it.hasNext();){
//				Int_MatchFeat_Arr oneVW_matches=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//				matches.add(new Int_MatchFeat_Arr(oneVW_matches.Integer,oneVW_matches.feats));
//			}
//			
//			//**************** save this doc into doc_scores_order ****************
//			float docScore=comparePhotos.scoreOneDoc(new DocAllMatchFeats(docID,matches.toArray(new Int_MatchFeat_Arr[0])), queryFeatReader, thisQueryID, null, disp, rerankFlag);
//			docScores.add(docScore);
//			docIDs.add(docID);
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
//			if (reduceNum!=0) {//this reducer has key-value assigned!
//				//do 1vs1 and HPM check on query's initial rank
//				ArrayList<Integer> topDocs=new ArrayList<Integer>(); ArrayList<Float> topScores=new ArrayList<Float>();
//				General_IR.rank_get_TopDocScores_PriorityQueue(docIDs, docScores, topRank, topDocs, topScores, "DES", true);
//				context.write(new IntWritable(queryID_previous), new IntList_FloatList(topDocs,topScores));
//				System.out.println("last query: "+queryID_previous+", total selected photos in its initial rank: "+docIDs.size()+", saved top doc numbers:"+topDocs.size()
//						+", top10Docs:"+topDocs.subList(0, Math.min(10, topDocs.size()))+", top10Scores:"+topScores.subList(0, Math.min(10, topDocs.size())));
//				processedQueryNum++;
//			}
//			
//			// ***** finsihed ***//			
//			System.out.println("one Reducer finished! total querys in this reducer:"+processedQueryNum+", reduceNum:"+reduceNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
//			super.setup(context);
//	 	}
//	}
//
//	public static class Reducer_buildRank_final_saveDocMatches extends Reducer<Key_QID_DID,Int_MatchFeat_Arr,IntWritable,DID_Score_ImageRegionMatch_ShortArr_Arr>  {
//		
//		private int topRank;
//		private int processedQueryNum;
//		private int reduceNum;
//		private long startTime;
//		private int dispInter;
//		private String rerankFlag;
//		private int reRankByHPM;
//		
//		private MapFile.Reader queryFeatReader;
//		
//		private ComparePhotos_LocalFeature comparePhotos;
//		
//		private ArrayList<DID_Score_ImageRegionMatch_ShortArr> docInfos;
//		private ArrayList<Float> docScores;
//		private int queryID_previous;
//		private boolean disp;
//		
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf = context.getConfiguration();
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("docInfo.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//***** select top rank for output ***//
//			topRank=Integer.valueOf(conf.get("mapred.topRank")); //select top rank as output
//			System.out.println("select top-"+topRank+" in rankList as output");
//			//***** set rerankFlag ***********
//			rerankFlag=conf.get("mapred.rerankFlag");
//			System.out.println("rerankFlag:"+rerankFlag);
//			//***** set reRankByHPM for 1vs1&HPM rerank ***//
//			reRankByHPM=Integer.valueOf(conf.get("mapred.reRankByHPM")); //select top rank to do 1vs1 and HPM check
//			System.out.println("select top-"+reRankByHPM+" in the initial rankList to do 1vs1 and HPM check!");
//			//***** read query feats ***//
//			queryFeatReader=General_Hadoop.openMapFileInNode("data", conf, true);
//			System.out.println("open query-Feat-MapFile finished");
//			//********** setup ComparePhotos_LocalFeature ***************
//			comparePhotos=new ComparePhotos_LocalFeature();
//			System.out.println("current memory:"+General.memoryInfo());
//			//-setup_scoreDoc
//			String info_setup_scoreDoc= comparePhotos.setup_scoreDoc(Integer.valueOf(conf.get("mapred.HMDistThr_rankDoc")), Double.valueOf(conf.get("mapred.HMWeight_deta")), 
//					Integer.valueOf(conf.get("mapred.HPM_level")), "docInfo.file", "TVectorInfo.file", conf);
//			System.out.println(info_setup_scoreDoc);
//			System.out.println("current memory:"+General.memoryInfo());
//			//***** set queryID_previous ***//
//			docInfos=new ArrayList<DID_Score_ImageRegionMatch_ShortArr>(reRankByHPM*2); //save all docs' score for one query
//			docScores=new ArrayList<Float>(reRankByHPM*2); 
//			queryID_previous=-1;
//			// ***** setup finsihed ***//
//			processedQueryNum=0;
//			reduceNum=0;
//			startTime=System.currentTimeMillis();
//			dispInter=10;
//			disp=true;
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
//				ArrayList<DID_Score_ImageRegionMatch_ShortArr> topDocs=new ArrayList<DID_Score_ImageRegionMatch_ShortArr>(topRank); ArrayList<Float> topScores=new ArrayList<Float>();
//				General_IR.rank_get_TopDocScores_PriorityQueue(docInfos, docScores, topRank, topDocs, topScores, "DES", true);
//				disp=false;
//				context.write(new IntWritable(queryID_previous), new DID_Score_ImageRegionMatch_ShortArr_Arr(topDocs));
//				//disp progress
//				processedQueryNum++;
//				if (processedQueryNum%dispInter==0){ 
//					System.out.println("\n"+processedQueryNum+" querys finished! time:"+General.dispTime(System.currentTimeMillis()-startTime, "min")
//							+", current finished queryName: "+queryID_previous+", total selected photos in its initial rank: "+docInfos.size()+", saved top doc numbers:"+topDocs.size()
//							+", top1Doc_Score:"+topDocs.get(0).dID_score.toString()+", top10Scores:"+topScores.subList(0, Math.min(10, topDocs.size())));
//					disp=true;
//				}
//				//prepareForNext
//				docInfos.clear(); docScores.clear();
//			}
//
//			//********combine all vw_MatchFeats for one doc************
//			int docID=QID_DID.docID;
//			ArrayList<Int_MatchFeat_Arr>  allMatches = new ArrayList<Int_MatchFeat_Arr>(); 
//			for(Iterator<Int_MatchFeat_Arr> it=vw_MatchFeats.iterator();it.hasNext();){
//				Int_MatchFeat_Arr oneVW_matches=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//				allMatches.add(new Int_MatchFeat_Arr(oneVW_matches.Integer,oneVW_matches.feats));
//			}
//			
//			//**************** get matches' weights ****************
//			ArrayList<ImageRegionMatch> goodMatches=new ArrayList<ImageRegionMatch>(); 
//			float docScore=comparePhotos.scoreOneDoc(new DocAllMatchFeats(docID,allMatches.toArray(new Int_MatchFeat_Arr[0])), queryFeatReader, thisQueryID, goodMatches, disp, rerankFlag);
//			docScores.add(docScore);
//			docInfos.add(new DID_Score_ImageRegionMatch_ShortArr(docID, docScore, goodMatches));
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
//			if (reduceNum!=0) {//this reducer has key-value assigned!
//				//do 1vs1 and HPM check on query's initial rank
//				ArrayList<DID_Score_ImageRegionMatch_ShortArr> topDocs=new ArrayList<DID_Score_ImageRegionMatch_ShortArr>(); ArrayList<Float> topScores=new ArrayList<Float>();
//				General_IR.rank_get_TopDocScores_PriorityQueue(docInfos, docScores, topRank, topDocs, topScores, "DES", true);
//				context.write(new IntWritable(queryID_previous), new DID_Score_ImageRegionMatch_ShortArr_Arr(topDocs));
//				System.out.println("last query: "+queryID_previous+", total selected photos in its initial rank: "+docInfos.size()+", saved top doc numbers:"+topDocs.size()
//						+", top10Docs:"+topDocs.subList(0, Math.min(10, topDocs.size()))+", top10Scores:"+topScores.subList(0, Math.min(10, topDocs.size())));
//				processedQueryNum++;
//			}
//			
//			// ***** finsihed ***//			
//			System.out.println("one Reducer finished! total querys in this reducer:"+processedQueryNum+", reduceNum:"+reduceNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
//			super.setup(context);
//	 	}
//	}
//
//}
