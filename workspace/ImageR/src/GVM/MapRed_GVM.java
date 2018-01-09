package GVM;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.Geo.GVM_Carto;
import MyAPI.Geo.GVM_Evaluator;
import MyAPI.Geo.GVM_Evaluator_Carto;
import MyAPI.Geo.GVM_Evaluator_Loc;
import MyAPI.Geo.GVM_Loc;
import MyAPI.Geo.groupDocs.CartoDocs;
import MyAPI.Geo.groupDocs.CartoListProc;
import MyAPI.Geo.groupDocs.GroupEstResult;
import MyAPI.Geo.groupDocs.LocDocs;
import MyAPI.Geo.groupDocs.LocListProc;
import MyAPI.Geo.groupDocs.UserIDs;
import MyAPI.Obj.Disp;
import MyAPI.Obj.FindEqualSizedBin;
import MyAPI.Obj.Statistics;
import MyCustomedHaoop.KeyClass.Key_RankFlagID_QID;
import MyCustomedHaoop.MapRedFunction.MapRed_countDataNum;
import MyCustomedHaoop.Mapper.Mapper_replication;
import MyCustomedHaoop.Mapper.Mapper_replication.MapperReplication_RankFlagID_QID_keyV;
import MyCustomedHaoop.Mapper.Mapper_replication.MapperReplication_RankFlagID_QID_valueV;
import MyCustomedHaoop.Partitioner.Partitioner_KeyisPartID_PartKey;
import MyCustomedHaoop.Partitioner.Partitioner_random;
import MyCustomedHaoop.Reducer.Reducer_combineReport;
import MyCustomedHaoop.ValueClass.ComposeWritableClassCollection.PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;
import MyCustomedHaoop.ValueClass.GVMres;

public class MapRed_GVM extends Configured implements Tool{

	/** 
	 * 
	 * @throws Exception 
	 * @command_example: 
	 * 
	 * ME15TMM:	yarn jar MapRed_GVM.jar GVM.MapRed_GVM -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,JSAT_my.jar,lire136.jar -DGVMVariant=GVMloc  -Dlatlons=MediaEval15/ME15_photos_latlons.floatArr -DuserIDs_0=MediaEval15/ME15_photos_userIDs_0.long -DuserIDs_1=MediaEval15/ME15_photos_userIDs_1.int -DG_ForGTSize=1 -DV_ForGTSize=1000 -DevalFlag=DivideQ@1,2,3,5,10@5000,10000,15000,20000@0,1,5,10,20,40,100_1,10,100 -DreRankScales=1000 -DvisScales=1000 -DgeoExpanScales=0.01 -DisNoSameUser=true -Dis1U1P=false -DGVM_matScoThrs=0,2,3 -DGVM_smooth=5 -DGVM_blockGrFeat=0,30,20,10 -DGVM_blockIDF=0,30,20,10 -DisExpan=1vs1,bestDoc,sumDoc -DfreqThrs=3,5,6,7,8 -DactQNum=-1 _Q0_65KVWSIFTUPRightOxford1_HD46@30_BurstIntraInter_PGM@0.52@0.2-1000 MediaEval15/ranks/Q2014-0_HR1000_ME15_DVW_SIFTUPRightOxford1_VW65K_iniR-BurstIntraInter@46@30_ReR1K_reRHE@46@30_Top1K_1vs1AndHistAndAngle@true@true@true@0.52@0.2@1@0@0@0@0@0@0@0_rankDocMatches MediaEval15/GVM/ 5000 0
				yarn jar MapRed_GVM.jar GVM.MapRed_GVM -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,JSAT_my.jar,lire136.jar -DGVMVariant=GVMloc  -Dlatlons=MediaEval15/ME15_photos_latlons.floatArr -DuserIDs_0=MediaEval15/ME15_photos_userIDs_0.long -DuserIDs_1=MediaEval15/ME15_photos_userIDs_1.int -DG_ForGTSize=1 -DV_ForGTSize=1000 -DevalFlag=DivideQ@1,2,3,4,5,6,7,8,9,10@5000,10000,15000,20000@0,1,5,10,20,40,100_1,10,100 -DtargetSchemes=_VisNN@0_reRGroup@100@100@-1.0@true@false,_GVR@0_reRGroup@1000@1000@0.01@true@false,_GVM@0@5@30@0@bestDoc@7_reRGroup@1000@1000@0.01@true@false,_GVM@0@5@30@0@1vs1@6_reRGroup@1000@1000@0.01@true@false,_GVM@0@5@30@0@sumDoc@6_reRGroup@1000@1000@0.01@true@false -DactQNum=-1 _Q0_65KVWSIFTUPRightOxford1_HD46@30_BurstIntraInter_PGM@0.52@0.2-1000 MediaEval15/ranks/Q2014-0_HR1000_ME15_DVW_SIFTUPRightOxford1_VW65K_iniR-BurstIntraInter@46@30_ReR1K_reRHE@46@30_Top1K_1vs1AndHistAndAngle@true@true@true@0.52@0.2@1@0@0@0@0@0@0@0_rankDocMatches MediaEval15/GVM/ 5000 0
	 * SanFra:	yarn jar MapRed_GVM.jar GVM.MapRed_GVM -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,JSAT_my.jar,lire136.jar -DGVMVariant=GVMcart -DcartoIDs_db=ImageR/BenchMark/SanFrancisco/SanFrancisco_Q-DPCI_cartoIDs_db.intArr -DcartoIDs_Q=ImageR/BenchMark/SanFrancisco/SanFrancisco_Q-DPCI_cartoIDs_q_corr2014.hashSetArr -DgTruths=ImageR/BenchMark/SanFrancisco/SanFrancisco_groundTruth_onlyPCI_inSInd_corr2014.hashMap -DV_ForGTSize=1000 -DevalFlag=DivideQ@1,2,3,5,10@0,1,5,10,20,40,100@0,1,5,10,20,40,100 -DreRankScales=1000 -DvisScales=1000 -DGVM_matScoThrs=3,4,5 -DGVM_smooth=5 -DGVM_blockGrFeat=0,30,20,10 -DGVM_blockIDF=0,30,20,10 -DisExpan=1vs1,bestDoc,sumDoc -DfreqThrs=1,2,3,4,5,6,7,8,-1 -DactQNum=803 _Q0_65KVWSIFTUPRightOxford1_HD46@30_BurstIntraInter_PGM@0.26@0.2-1000 ImageR/BenchMark/SanFrancisco/ranks/HR1000_SanFran_DPCI_QDPCIVW_SIFTUPRightOxford1_VW65K_iniR-BurstIntraInter@46@30_ReR1K_reRHE@46@30_Top1K_1vs1AndHistAndAngle@true@true@true@0.26@0.2@1@0@0@0@0@0@0@0_rankDocMatches ImageR/BenchMark/SanFrancisco/GVM/ 2000 0
				yarn jar MapRed_GVM.jar GVM.MapRed_GVM -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,JSAT_my.jar,lire136.jar -DGVMVariant=GVMcart -DcartoIDs_db=ImageR/BenchMark/SanFrancisco/SanFrancisco_Q-DPCI_cartoIDs_db.intArr -DcartoIDs_Q=ImageR/BenchMark/SanFrancisco/SanFrancisco_Q-DPCI_cartoIDs_q_corr2014.hashSetArr -DgTruths=ImageR/BenchMark/SanFrancisco/SanFrancisco_groundTruth_onlyPCI_inSInd_corr2014.hashMap -DV_ForGTSize=1000 -DevalFlag=DivideQ@1,2,3,4,5,6,7,8,9,10@0,1,5,10,20,40,100@0,1,5,10,20,40,100 -DtargetSchemes=_VisNN@4_reRGroup@100@100,_GVR@4_reRGroup@1000@1000,_GVM@4@5@0@30@bestDoc@5_reRGroup@1000@1000,_GVM@4@5@20@10@1vs1@5_reRGroup@1000@1000,_GVM@4@5@0@20@sumDoc@2_reRGroup@1000@1000 -DactQNum=803 _Q0_65KVWSIFTUPRightOxford1_HD46@30_BurstIntraInter_PGM@0.26@0.2-1000 ImageR/BenchMark/SanFrancisco/ranks/HR1000_SanFran_DPCI_QDPCIVW_SIFTUPRightOxford1_VW65K_iniR-BurstIntraInter@46@30_ReR1K_reRHE@46@30_Top1K_1vs1AndHistAndAngle@true@true@true@0.26@0.2@1@0@0@0@0@0@0@0_rankDocMatches ImageR/BenchMark/SanFrancisco/GVM/ 2000 0
	 */
	
	public static String[] oriArgs; //save all arguments
	
	Configuration conf;
	FileSystem hdfs;
	ArrayList<String> taskLabels;
	String baseRankLabel;
	boolean saveRes;
	PrintWriter outStr_report;
	String visRankPath;
	String workDir;
	String geoRedunBinInfoPath;
	
	public static void main(String[] args) throws Exception {
		runOnHadoop(args);
		
//		analysisReport();
	}
	
	public static void analysisReport() throws IOException, InterruptedException{
//		String reportPath="F:/Experiments/MediaEval15/GVM/LocAsVecReport_Q0_65KVWSIFTUPRightOxford1_HD46@30_BurstIntraInter_PGM@0.52@0.2-1000";
//		String reportPath="F:/Experiments/SanFrancisco/GVM/LocAsVecReport_65KVWSIFTUPRightOxford1_HD46@30_BurstIntraInter_PGM@0.26@0.2-1000";
		String reportPath="/home/yiran/Desktop/MapRed_GVM/turnOnDev80Q.report";
		BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(reportPath), "UTF-8"));
//		int lineInter=20;//20 for 1km precision, 41 for 10km, 62 for 100km in MediaEval15
		int lineInter=18;//21 for SanFrancisco
		String oneScheme=null; Statistics<String> stat=new Statistics<String>(300); 
		while ((oneScheme=inStr_photoMeta.readLine())!=null) {
			if (oneScheme.startsWith("_GVM")) {//_VisNN@0_reRGroup@100@100@-1.0@true@false
				String[] schemeLabel_groupLabel=oneScheme.split("_");
//				if (schemeLabel_groupLabel[2].split("@")[3].equalsIgnoreCase("0.01")) {//select rules for ME15
//				if (schemeLabel_groupLabel[2].split("@")[3].equalsIgnoreCase("0.01") && schemeLabel_groupLabel[1].split("@")[5].startsWith("sumDoc")) {//select rules for ME15
				if (schemeLabel_groupLabel[1].split("@")[5].startsWith("1vs1")) {//select rules
					String oneLine=null;
					for (int i = 0; i < lineInter; i++) {
						oneLine=inStr_photoMeta.readLine();
					}
					stat.addSample(Float.valueOf(oneLine.split("\t")[0]), oneScheme);
//					System.out.println(oneScheme+": "+oneLine);
				}
			}
		}
		inStr_photoMeta.close();
		System.out.println("done! stat:"+stat.getFullStatistics("0.0000", true));
	}
	
	public static void runOnHadoop(String[] args) throws Exception{
		oriArgs=args;
		int ret = ToolRunner.run(new MapRed_GVM(), args);
		System.exit(ret);
	}

	public static ArrayList<String> makeTaskLabels(Configuration conf, boolean isGVMloc){
		ArrayList<String> taskLabels=new ArrayList<>();
		String tragetSchemes=conf.get("targetSchemes");
		if (tragetSchemes!=null) {//for spicific schemes
			taskLabels=new ArrayList<>(Arrays.asList(tragetSchemes.split(",")));
		}else{
			//parameters for locListProc, -DreRankScales=10,20,50,100, -DvisScales=10,20,50,100, -DgeoExpanScales=0.001,0.01,0.1, -DisNoSameUser=true, -Dis1U1P=true,false
			int[] reRankScales=General.StrArrToIntArr(conf.get("reRankScales").split(",")); //10,20,50,100
			int[] visScales=General.StrArrToIntArr(conf.get("visScales").split(",")); //10,20,50,100
			//parameters for GVM
			int[] GVM_matchScoreThrs=General.StrArrToIntArr(conf.get("GVM_matScoThrs").split(",")); //0,1,2,3
			int[] GVM_smoothFactors=General.StrArrToIntArr(conf.get("GVM_smooth").split(",")); //10,20,30,50,100
			int[] GVM_blockPortions_groupFeat=General.StrArrToIntArr(conf.get("GVM_blockGrFeat").split(",")); //2,4,10,20,40,100
			int[] GVM_blockPortions_IDF=General.StrArrToIntArr(conf.get("GVM_blockIDF").split(",")); //2,4,10,20,40,100
			String[] isExpans=conf.get("isExpan").split(",");//true,false
			int[] freqThrs=General.StrArrToIntArr(conf.get("freqThrs").split(","));//1,2,3,4
			//make taskLabels	
			if (isGVMloc) {
				float[] geoExpanScales=General.StrArrToFloatArr(conf.get("geoExpanScales").split(","));//0.001,0.01,0.1
				boolean[] isNoSameUsers=General.StrArrToBooleanArr(conf.get("isNoSameUser").split(","));//true,false;
				boolean[] is1U1Ps=General.StrArrToBooleanArr(conf.get("is1U1P").split(","));//true,false
				for (boolean isNoSameUser : isNoSameUsers) {
					for (int thr : GVM_matchScoreThrs) {
						taskLabels.add("_VisNN@"+thr+LocListProc.setLocListParams(-1, 100, -1, isNoSameUser, false));
					}
					for (boolean is1U1P : is1U1Ps){
						for (int reRankScale:reRankScales){
							for (int visScale:visScales){ // loop over parameters
								if ((reRankScale<=visScale)) {
									for(double geoExpanScale:geoExpanScales){
										String locListPara=LocListProc.setLocListParams(reRankScale, visScale, (float) geoExpanScale, isNoSameUser, is1U1P);
										for (int thr : GVM_matchScoreThrs) {
											//*********  run GVR ***************
											taskLabels.add("_GVR@"+thr+locListPara);//
										}
									}
								}
							}
						}
					}
					for (boolean is1U1P : is1U1Ps){
						for (int reRankScale:reRankScales){
							for (int visScale:visScales){ // loop over parameters
								if ((reRankScale<=visScale)) {
									for(double geoExpanScale:geoExpanScales){
										String locListPara=LocListProc.setLocListParams(reRankScale, visScale, (float) geoExpanScale, isNoSameUser, is1U1P);
										for (int thr : GVM_matchScoreThrs) {
											//*********  run GVM ***************
											for (int smoothFactor:GVM_smoothFactors){
												for (int blockPortion_gf:GVM_blockPortions_groupFeat){
													for (int blockPortion_idf:GVM_blockPortions_IDF){
														for (String isExpan : isExpans){
															for (int freqThr : freqThrs){
																taskLabels.add("_GVM@"+thr+"@"+smoothFactor+"@"+blockPortion_gf+"@"+blockPortion_idf+"@"+isExpan+"@"+freqThr+locListPara);//
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}else {
				for (int thr : GVM_matchScoreThrs) {
					taskLabels.add("_VisNN@"+thr+CartoListProc.setGroupListParams(-1, 100));
				}
				for (int reRankScale:reRankScales){
					for (int visScale:visScales){ // loop over parameters
						if ((reRankScale<=visScale)) {
							String locListPara=CartoListProc.setGroupListParams(reRankScale, visScale);
							for (int thr : GVM_matchScoreThrs) {
								//*********  run GVR ***************
								taskLabels.add("_GVR@"+thr+locListPara);//
							}
						}
					}
				}
				for (int reRankScale:reRankScales){
					for (int visScale:visScales){ // loop over parameters
						if ((reRankScale<=visScale)) {
							String locListPara=CartoListProc.setGroupListParams(reRankScale, visScale);
							for (int thr : GVM_matchScoreThrs) {
								//*********  run GVM ***************
								for (int smoothFactor:GVM_smoothFactors){
									for (int blockPortion_gf:GVM_blockPortions_groupFeat){
										for (int blockPortion_idf:GVM_blockPortions_IDF){
											for (String isExpan : isExpans){
												for (int freqThr : freqThrs){
													taskLabels.add("_GVM@"+thr+"@"+smoothFactor+"@"+blockPortion_gf+"@"+blockPortion_idf+"@"+isExpan+"@"+freqThr+locListPara);//
												}
											}
										}
									}
								}
							}
						}
					}
				}
				
			}
		}
		return taskLabels;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		conf = getConf();
		hdfs=FileSystem.get(conf);
		String[] otherArgs = args; //use this to parse args!
		String dateFormate="yyyy.MM.dd G 'at' HH:mm:ss z";
		//set common
		boolean isGVMloc=conf.get("GVMVariant").equalsIgnoreCase("GVMloc");//GVMloc, GVMcart
		
		baseRankLabel=otherArgs[0];//_D9M_20KVW_SURFHD20-12-20_BurstIntraInter_1vs1AndHistAndAngle@0.52@0.2-1000
		visRankPath=otherArgs[1];//MM15/ImageR/ranks/Q0_MEva13_9M_20K-VW_SURF_iniR-BurstIntraInter_HDs20-HMW12_ReR1K_HDr20_top1K_1vs1AndHistAndAngle@0.52@0.2@1@0@0@0@0@0@0@0_rankDocMatches
		workDir=otherArgs[2];//MM15/GVM/
		int RedNumForProcRank=Integer.valueOf(otherArgs[3]);//for paralise process schemes and queries
		int startStage=Integer.valueOf(otherArgs[4]);
		//report
		outStr_report=new PrintWriter(new OutputStreamWriter(hdfs.create(new Path(workDir+"Report"+baseRankLabel),false), "UTF-8"),true); 
		General.dispInfo(outStr_report,"oriArgs:"+General.StrArrToStr(oriArgs, " ")+"\n");
		General.dispInfo(outStr_report,"...............baseRankLabel:"+baseRankLabel+"   ............. ............. ");
			
//		geoRedunBinInfoPath=Conf_General.hdfs_address+workDir+"geoRedunBinInfo"+baseRankLabel+".InfoStr";
		
		//makeTaskLabels
		taskLabels=makeTaskLabels(conf, isGVMloc);
		boolean isReplicateRank=taskLabels.size()<150;//not need to distribute all ranks to all nodes
		General.dispInfo(outStr_report,General.dispTimeDate(System.currentTimeMillis(), dateFormate)+", start processing "+taskLabels.size()+" taskLabels!   isReplicateRank: "+isReplicateRank+" ..................");

		//*********  run all taskLabels ***************
		String rankFlagsData=workDir+"GVM_taskLabels.arrList";
		General_Hadoop.writeObject_HDFS(hdfs, rankFlagsData, taskLabels);
		//set commons
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		String out_reranked=workDir+"GVM_for"+baseRankLabel; //output path for reranked ranks
		
		//******* 1st job: run all taskLabels for all queries in one job  ******
		//set input/output path
		String job1_out=out_reranked+"_reRank";
		//set taskLabelNum
		Mapper_replication.setReplicationNum(conf, taskLabels.size());
		//set distributed cache, add latlons and concept Mapfile to Distributed cache
		cacheFilePaths.clear();
		cacheFilePaths.add(rankFlagsData+"#rankFlagsData.file"); //rankFlagsData path with symLink
		if (isGVMloc) {
			cacheFilePaths.add(conf.get("latlons")+"#latlons.file"); //latlons path with symLink
			cacheFilePaths.add(conf.get("userIDs_0")+"#userIDs_0.file"); //userIDs_0
			cacheFilePaths.add(conf.get("userIDs_1")+"#userIDs_1.file"); //userIDs_1
		}else {
			cacheFilePaths.add(conf.get("cartoIDs_db")+"#cartoIDs_db.file"); //latlons path with symLink
			cacheFilePaths.add(conf.get("cartoIDs_Q")+"#cartoIDs_Q.file"); //userIDs_0
			cacheFilePaths.add(conf.get("gTruths")+"#gTruths.file"); //userIDs_1
		}
		//set reducer number
		int job1RedNum=RedNumForProcRank; //reducer number
		//run
		if (isReplicateRank) {
			General_Hadoop.Job(startStage<=0, conf, new Path[]{new Path(visRankPath+"/data")}, job1_out, "process", job1RedNum, 8, 2, true,
					MapRed_GVM.class, MapperReplication_RankFlagID_QID_valueV.class, Partitioner_random.class, null, null, Reducer_processRankDocMatches_rankAsV.class,
					Key_RankFlagID_QID.class, PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr.class, Key_RankFlagID_QID.class, GVMres.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0L, 0,
					cacheFilePaths.toArray(new String[0]),null);
		}else {
			cacheFilePaths.add(visRankPath+"#iniVisRank.mapFile"); //rankFlagsData path with symLink
			General_Hadoop.Job(startStage<=0, conf, new Path[]{new Path(visRankPath+"/data")}, job1_out, "process", job1RedNum, 8, 2, true,
					MapRed_GVM.class, MapperReplication_RankFlagID_QID_keyV.class, Partitioner_random.class, null, null, Reducer_processRankDocMatches_rankInCache.class,
					Key_RankFlagID_QID.class, Key_RankFlagID_QID.class, Key_RankFlagID_QID.class, GVMres.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0L, 0,
					cacheFilePaths.toArray(new String[0]),null);
		}
		
		
		//******* 2nd job: getEquBinForGeoRedundency ******
		//run getEquBinForGeoRedundency
//		if (!hdfs.exists(new Path(geoRedunBinInfoPath))) {//this is the first time to make geoRedunBinInfo
//			conf.set("InfoStrPath",geoRedunBinInfoPath); // save String object to InfoStrPath
//			General_Hadoop.Job(conf, new Path[]{new Path(job1_out)}, null, "getEquBin", 1, 8, 2, false,
//					MapRed_GVM.class, null, null,null,null,Reducer_getEquBinForGeoRedundency.class,
//					IntWritable.class, GVMres.class, IntWritable.class,GVMres.class,
//					SequenceFileInputFormat.class, NullOutputFormat.class, 0L, 0,
//					null,null);
//		}
//		String Info=(String) General_Hadoop.readObject_HDFS(hdfs, geoRedunBinInfoPath);
//		conf.set("binsForGeoRedun",Info); //for Job3		
		
		//******* 3nd job: anaylisis, 1 taskLabel 1 reduecer ******
		int job3RedNum=taskLabels.size();
		String reportPath=out_reranked+"_reports";
		System.out.println(reportPath);
		if (startStage<=1) {
			//***** preJob: count queryNum in iniVisual rank ************
			int actQNum=Integer.valueOf(conf.get("actQNum"));
			int queryNumInVisRank=MapRed_countDataNum.runHadoop(conf, new Path[]{new Path(visRankPath+"/data")}, workDir) ;
			General.dispInfo(outStr_report,"actQNum: "+actQNum+", queryNumInVisRank: "+queryNumInVisRank);
			if (actQNum==-1) {//actQNum is not setted, use queryNumInVisRank instead
				conf.set("actQNum", queryNumInVisRank+"");
			}
			//run
			cacheFilePaths.clear();
			cacheFilePaths.add(rankFlagsData+"#rankFlagsData.file"); //rankFlagsData path with symLink
			if (isGVMloc) {
				cacheFilePaths.add(conf.get("latlons")+"#latlons.file"); //latlons path with symLink
			}else {
				cacheFilePaths.add(conf.get("cartoIDs_Q")+"#cartoIDs_Q.file"); //userIDs_0
			}
			General_Hadoop.Job(conf, new Path[]{new Path(job1_out)}, reportPath, "analysis", job3RedNum, 8, 2, true,
					MapRed_GVM.class, null, Partitioner_KeyisPartID_PartKey.class, null, null, Reducer_makeReport.class,
					Key_RankFlagID_QID.class, GVMres.class, IntWritable.class, Text.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0L, 0,
					cacheFilePaths.toArray(new String[0]),null);
		}
		//combine
		if (startStage<=2) {
			Reducer_combineReport.Job_combineReports(conf, reportPath, outStr_report, MapRed_GVM.class, "combineReports");
		}
		
		//********* clean-up ***********//
		hdfs.delete(new Path(job1_out), true);
		hdfs.delete(new Path(reportPath), true);
		
		General.closePrintWriterOnExist(outStr_report);
//		General_Hadoop.deleteIfExist(geoRedunBinInfoPath, hdfs);
		General_Hadoop.deleteIfExist(rankFlagsData, hdfs);//clean reRankFlags ArrayList
		hdfs.close();
		return 0;
	}
	


	//******* 1st job: query rerank  ******
	public static abstract class Reducer_processRankDocMatches <V extends Writable> extends Reducer<Key_RankFlagID_QID,V,Key_RankFlagID_QID,GVMres>  {
		
		//for calculating groundTruth
		private float G_ForGTSize;
		private int V_ForGTSize;
		//GVM
		boolean isGVMloc;// GVMloc or GVMcarto
		//for GVMloc
		private UserIDs userIDs;
		private float[][] latlons;
		GVM_Evaluator_Loc eval_loc;
		//for GVMcarto
		int[] cartoIDs_db;
		HashSet<Integer>[] cartoIDs_Q;
		HashMap<Integer, HashSet<Integer>> gTruths;
		GVM_Evaluator_Carto eval_carto;
		//GVM Parameters
		ArrayList<String> GVMTaskLabels;
		//common
		private int queryNum;
		private int queryNum_noRes;
		private boolean disp;
		private int dispInter;
		
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//******* read taskLabel**************
			isGVMloc=conf.get("GVMVariant").equalsIgnoreCase("GVMloc");//GVMloc, GVMcart
			GVMTaskLabels=(ArrayList<String>) General.readObject("rankFlagsData.file");
			System.out.println("GVMTaskLabels loaded, size: "+GVMTaskLabels.size());
			//check file in distributted cache
			General.checkDir(new Disp(true,"",null), ".");
			//config
			String evalFlag=conf.get("evalFlag");
			if (isGVMloc) {
				G_ForGTSize=Float.valueOf(conf.get("G_ForGTSize"));
				latlons=(float[][]) General.readObject("latlons.file");
				userIDs=new UserIDs((long[]) General.readObject("userIDs_0.file"),(int[]) General.readObject("userIDs_1.file")); 
				eval_loc=new GVM_Evaluator_Loc(evalFlag, "latlons.file"); //noDivideQ@1,3,5,10_1,10,100
			}else {
				cartoIDs_db=(int[]) General.readObject("cartoIDs_db.file");
				cartoIDs_Q=(HashSet<Integer>[]) General.readObject("cartoIDs_Q.file");
				gTruths=(HashMap<Integer, HashSet<Integer>>) General.readObject("gTruths.file");
				eval_carto=new GVM_Evaluator_Carto(evalFlag, "cartoIDs_Q.file");
			}
			//******* read G_ForGTSize, V_ForGTSize**************		
			V_ForGTSize=Integer.valueOf(conf.get("V_ForGTSize"));
			System.out.println("G_ForGTSize: "+G_ForGTSize+", V_ForGTSize:"+V_ForGTSize);
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			disp=false;
			queryNum=0; //total point number in one reducer
			queryNum_noRes=0;
			dispInter=1000;
			
	 	}
		
		@Override
		public void reduce(Key_RankFlagID_QID flagID_QID, Iterable<V> value, Context context) throws IOException, InterruptedException {
			PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr oneQVisRank=getIniVisRank(flagID_QID, value);
			String taskLabel=GVMTaskLabels.get(flagID_QID.obj_1.get());
			if (queryNum%dispInter==0) {
				disp=true;
				System.out.println("\t start process "+queryNum+"-th query! its flagID_QID: "+flagID_QID+", taskLabel: "+taskLabel);
			}
			int groundTrueSize=0; int topVisRankedGTruthNum=0; int[] firstTrueRank=null; 
			if (isGVMloc) {
				GVM_Loc gvm_Loc=new GVM_Loc(taskLabel, userIDs, latlons, eval_loc.num_topGroups, eval_loc.num_topGroups, null, null, G_ForGTSize, V_ForGTSize, null);
				GroupEstResult<LocDocs<DID_Score_ImageRegionMatch_ShortArr>> res=gvm_Loc.ProcessOneQuery(flagID_QID.obj_2.get(), oneQVisRank, disp);
				if (res!=null) {
					groundTrueSize=res.queryReduncy;
					topVisRankedGTruthNum=res.topVisRankedGTruthNum;
					firstTrueRank=eval_loc.evalOneQuery_loc(flagID_QID.obj_2.get(), LocDocs.getLocationList(res.res));
				}
				
			}else{
				GVM_Carto gvm_Carto=new GVM_Carto(taskLabel, cartoIDs_Q, cartoIDs_db, gTruths, eval_carto.num_topGroups, eval_carto.num_topGroups, null, null, V_ForGTSize, null);
				GroupEstResult<CartoDocs<DID_Score_ImageRegionMatch_ShortArr>> res=gvm_Carto.ProcessOneQuery(flagID_QID.obj_2.get(), oneQVisRank, disp);
				if (res!=null) {
					groundTrueSize=res.queryReduncy;
					topVisRankedGTruthNum=res.topVisRankedGTruthNum;
					firstTrueRank=eval_carto.evalOneQuery_carto(flagID_QID.obj_2.get(), res.res);
				}
			}
			if (firstTrueRank!=null) {
				context.write(flagID_QID, new GVMres(groundTrueSize, topVisRankedGTruthNum, firstTrueRank));
			}else {//no result queries
				queryNum_noRes++;
			}
			queryNum++;
			disp=false;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one reducer finished! total query number: "+queryNum+", queryNum_noRes:"+queryNum_noRes);
	 	}
		
		public abstract PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr getIniVisRank(Key_RankFlagID_QID key, Iterable<V> value) throws IOException;
	}

	public static class Reducer_processRankDocMatches_rankAsV extends Reducer_processRankDocMatches<PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr>  {
	
		@Override
		public PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr getIniVisRank(Key_RankFlagID_QID key, Iterable<PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr> value) {
			PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr oneQVisRank=General_Hadoop.readOnlyOneElement(value, key+"");
			return oneQVisRank;
		}
	}

	public static class Reducer_processRankDocMatches_rankInCache extends Reducer_processRankDocMatches<Key_RankFlagID_QID>  {
		
		//ini visual rank
		private MapFile.Reader iniVisRank;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//***** read query feats ***//
			iniVisRank=General_Hadoop.openMapFileInNode("iniVisRank.mapFile", conf, true);
			System.out.println("open iniVisRank.mapFile finished");
			super.setup(context);
	 	}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			iniVisRank.close();
			super.cleanup(context);
	 	}

		@Override
		public PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr getIniVisRank(Key_RankFlagID_QID key, Iterable<Key_RankFlagID_QID> value) throws IOException {
			PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr oneQVisRank=new PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr();
			iniVisRank.get(key.obj_2, oneQVisRank);
			return oneQVisRank;
		}
	}

	//******* 2nd job: getEquBinForGeoRedundency******
	public static class Reducer_getEquBinForGeoRedundency extends Reducer<IntWritable,GVMres,IntWritable,GVMres>  {
		private Configuration conf;
		private FileSystem hdfs;
		private FindEqualSizedBin findEqualSizedBin_geoRedun;
		private int queryNum;
		private String InfoStrPath;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			hdfs=FileSystem.get(conf);
			
			//******* setup findEqualSizedBin_geoRedun **************
			findEqualSizedBin_geoRedun=new FindEqualSizedBin(General.makeRange(General.StrArrToIntArr(conf.get("rangForBinsOfGeoRedun").split(","))), false);
			//**** set InfoStrPath ************//
			InfoStrPath=conf.get("InfoStrPath");
			
			// ***** setup finsihed ***//
			queryNum=0;
			System.out.println("only 1 reducer, findEqualSizedBin_geoRedun, save String obj to InfoStrPath: "+InfoStrPath);
			System.out.println("setup finsihed!");
			
	 	}
		
		@Override
		public void reduce(IntWritable Key_queryName, Iterable<GVMres> value, Context context) throws IOException, InterruptedException {
			//key: queryName, value: rank result
			queryNum++;
			//******** only one list in rank result! ************		
			GVMres oneResult = General_Hadoop.readOnlyOneElement(value, Key_queryName.toString());
			
			//******* analysis this query's result by different evaluation radius *********************
			int geoReduncy=oneResult.groundTrueSize;
			findEqualSizedBin_geoRedun.addOneSample(geoReduncy);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			findEqualSizedBin_geoRedun.makeRes(6, queryNum);
			//outPut
			String outInfo=General.IntArrToString(findEqualSizedBin_geoRedun.target_Bins, ",");
			General_Hadoop.writeObject_HDFS(hdfs, InfoStrPath, outInfo);
			
			// ***** setup finsihed ***//
			System.out.println("done! queryNum:"+queryNum+", outInfo: \n"+ outInfo);
			
	 	}
	}

	//******* 3rd job: analysis query result******
	public static class Reducer_makeReport extends Reducer<Key_RankFlagID_QID,GVMres, IntWritable, Text>  {
		//for calculating groundTruth
		private float G_ForGTSize;
		private int V_ForGTSize;
		//GVM
		boolean isGVMloc;// GVMloc or GVMcarto
		GVM_Evaluator eval;
		//GVM Parameters
		ArrayList<String> GVMTaskLabels;
		int thisRedTaskID;
		//common
		private int queryNum;
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			isGVMloc=conf.get("GVMVariant").equalsIgnoreCase("GVMloc");//GVMloc, GVMcart
			//******* read taskLabel**************
			GVMTaskLabels=(ArrayList<String>) General.readObject("rankFlagsData.file");
			System.out.println("GVMTaskLabels loaded, size: "+GVMTaskLabels.size());
			//config
			String evalFlag=conf.get("evalFlag");
			if (isGVMloc) {
				eval=new GVM_Evaluator_Loc(evalFlag, "latlons.file"); //noDivideQ@1,3,5,10_1,10,100
				G_ForGTSize=Float.valueOf(conf.get("G_ForGTSize"));
			}else {
				eval=new GVM_Evaluator_Carto(evalFlag, "cartoIDs_Q.file");
			}
			//******* read G_ForGTSize, V_ForGTSize**************
			V_ForGTSize=Integer.valueOf(conf.get("V_ForGTSize"));
			System.out.println("G_ForGTSize: "+G_ForGTSize+", V_ForGTSize:"+V_ForGTSize);
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			queryNum=0; //total point number in one reducer
			
	 	}
		
		@Override
		public void reduce(Key_RankFlagID_QID key, Iterable<GVMres> value, Context context) throws IOException, InterruptedException {
			//key: queryName, value: rank result
			
			//******** only one list in rank result! ************		
			int queryName=key.obj_2.get(); thisRedTaskID=key.obj_1.get(); GVMres rank = General_Hadoop.readOnlyOneElement(value, key.toString());
			//******* analysis this query's result by different evaluation radius *********************
			eval.addOneQueryRank(queryName, rank.firstTrueRank, rank.groundTrueSize, rank.topVisRankedGTruthNum);
			queryNum++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			int actQNum=Integer.valueOf(context.getConfiguration().get("actQNum"));
			String outInfo=GVMTaskLabels.get(thisRedTaskID)+"\n"+eval.getEvalationRes(actQNum);
			//outPut
			context.write(new IntWritable(thisRedTaskID), new Text(outInfo));
			
			// ***** setup finsihed ***//
			System.out.println("\n Reducer finished! total querys:"+queryNum+", thisRedTaskID:"+thisRedTaskID);
			System.out.println("outInfo: \n"+ outInfo);
			
	 	}
	}
		
	//******************************************************

}
