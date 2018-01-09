package RankStudy_MapRed;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.General.General_Lire;
import MyAPI.General.General_geoRank;
import MyAPI.Obj.Disp;
import MyAPI.Obj.FindEqualSizedBin;
import MyAPI.Obj.MakeHist_GVR;
import MyAPI.imagR.GTruth;
import MyCustomedHaoop.Partitioner.Partitioner_random;
import MyCustomedHaoop.Partitioner.Partitioner_random_sameKey;
import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_1key_1value;
import MyCustomedHaoop.ValueClass.FloatArr;
import MyCustomedHaoop.ValueClass.GeoExpansionData.fistMatch_GTruth_Docs_Locations;
import MyCustomedHaoop.ValueClass.DID_Score;
import MyCustomedHaoop.ValueClass.IntArr;
import MyCustomedHaoop.ValueClass.IntArr_FloatArr;
import MyCustomedHaoop.ValueClass.IntArr_byteArr;
import MyCustomedHaoop.ValueClass.IntList_FloatList;

public class MapRed_GeoVisualRanking extends Configured implements Tool{

	/** Different from MapRed_geoExpansion_getScore, no AP, but rank scores
	 * 
	 * job1: read query rank, and do geoExpansion, rerank, finally get query's groudTru-Size, top P doc (ori and reranked), their scores
	 * mapper: read and out, read query rank from MapFile(only use data), output: queryName_(docNames_scores)
	 * reducer: for one query, geoExpansion and rerank, output: sequence file queryName _ query's groudTru-Size, top P doc (ori and reranked), rank scores.
	 * 
	 * @param (Mapper_readRank):  "latlons" "userIDs_0" "userIDs_1" "topVisScale"  "geoExpanScale" "G_ForGTSize" "V_ForGTSize" "conceptThr" "conRank_thr" "FilterMapFile"
	 * 
	 * job2: combine results from job1
	 * mapper: read and output
	 * reducer: combine and save in MapFile, key:queryName
	 * 
	 * @param:  pho_S_to_L imgPath 
	 * 
	 * job3: analysis results from job1
	 * mapper: read and output
	 * reducer: analysis result and make report
	 * @param: "latlons" "reportPath"  "isSameLocScales"  "num_topLocations" "binsForGTSize" "accumLevel" "InfoStrPath"
	 * 
	 * 
	 * @throws Exception 
	 * @command_example: 
	 * ME13TMM:	hadoop jar MapRed_GeoVisualRanking.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,JSAT_my.jar,lire136.jar -Dlatlons=MediaEval13/MEval13_latlons.floatArr -DuserIDs_0=MediaEval13/MEval13_userIDs_0.long -DuserIDs_1=MediaEval13/MEval13_userIDs_1.int -DG_ForGTSize=0.01 -DV_ForGTSize=1000 -DFilterMapFile=MediaEval13/Global_PhoFeats/Gist_IM2GPS_MFile -DisSameLocScales=0.001,0.01,0.1,1  -Dnum_topLocations=20 -DbinsForGTSize=0,1,2,5,10,20 -DrangForBinsOfGeoRedun=0,5000,5 -DaccumLevel=1,2,3,5,10,20 -DsaveRes=false -DmakeReport=true _Vis_noGlobFilter_noSameUser_1U1P _D9M_Dev_SURFHD18-20-18_1vs1AndHPM6-1000 TMM_GVR/imagR/ranks/Dev_MEva13_9M_20K-VW_HDs18-HMW20_ReR1K_HDr18_top1K_1vs1AndHPM@6_rankDocScore/data TMM_GVR/GVR/ 700
	 * ME13TMM:	hadoop jar MapRed_GeoVisualRanking.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,JSAT_my.jar,lire136.jar -Dlatlons=MediaEval13/MEval13_latlons.floatArr -DuserIDs_0=MediaEval13/MEval13_userIDs_0.long -DuserIDs_1=MediaEval13/MEval13_userIDs_1.int -DG_ForGTSize=0.01 -DV_ForGTSize=1000 -DFilterMapFile=MediaEval13/Global_PhoFeats/Gist_IM2GPS_MFile -DisSameLocScales=0.001,0.01,0.1,1  -Dnum_topLocations=20 -DbinsForGTSize=0,1,2,5,10,20 -DrangForBinsOfGeoRedun=0,5000,5 -DaccumLevel=1,2,3,5,10,20 -DsaveRes=false -DmakeReport=true _Vis_noGlobFilter_noSameUser_1U1P _D9M_Q0_SURFHD18-20-18_1vs1AndHPM6-1000_Matches TMM_GVR/imagR/ranks/SURF_MEva13_9M_20K-VW_HDs18-HMW20_ReR1000_HDr18_top10K_1vs1AndHPM6_Q0_8_rankDocMatches TMM_GVR/GVR/ 700
	 * ME13TMM:	hadoop jar MapRed_GeoVisualRanking.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,JSAT_my.jar,lire136.jar -Dlatlons=MediaEval13/MEval13_latlons.floatArr -DuserIDs_0=MediaEval13/MEval13_userIDs_0.long -DuserIDs_1=MediaEval13/MEval13_userIDs_1.int -DG_ForGTSize=0.01 -DV_ForGTSize=1000 -DisSameLocScales=0.001,0.01,0.1,1  -Dnum_topLocations=20 -DbinsForGTSize=0,1,2,5,10,20 -DrangForBinsOfGeoRedun=0,5000,5 -DaccumLevel=1,2,3,5,10,20 -DsaveRes=false -DmakeReport=true _Vis_noGlobFilter_noSameUser_1U1P _D9M_Q0_Gist_IM2GPS TMM_GVR/imagR/ranks/Gist_IM2GPS_rank_top1K_Q0_8 TMM_GVR/GVR/ 700
	 * JaeVid:	hadoop jar MapRed_GeoVisualRanking.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,JSAT_my.jar,lire136.jar -Dlatlons=MediaEval13/Jaeyoung_video/VideoFrameQueryAndME9m_Latlons -DuserIDs_0=MediaEval13/Jaeyoung_video/VideoFrameQueryAndME9m_userIDs_0.long -DuserIDs_1=MediaEval13/Jaeyoung_video/VideoFrameQueryAndME9m_userIDs_1.int -DG_ForGTSize=0.01 -DV_ForGTSize=1000 -DisSameLocScales=0.001,0.01,0.1,1 -Dnum_topLocations=20 -DbinsForGTSize=0,1,2,5,10,20 -DrangForBinsOfGeoRedun=0,5000,5 -DaccumLevel=1,2,3,5,10,20 -DsaveRes=true -DmakeReport=true _Vis_noGlobFilter_noSameUser_1U1P _D9M_Video_SURFHD18-20-18_1vs1AndHPM6-1000 MediaEval13/Jaeyoung_video/ranks/SURF_MEva13_9M_20K-VW_HDs18-HMW20_ReR1K_HDr18_top1K_1vs1AndHPM@6_rankDocScore/data MediaEval13/Jaeyoung_video/GVR/ 300
	 * SanFra:	hadoop jar MapRed_GeoVisualRanking.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,JSAT_my.jar,lire136.jar -Dlatlons=ImageR/BenchMark/SanFrancisco/SanFrancisco_Q-DPCI_latlons.floatArr -DG_ForGTSize=0.005 -DV_ForGTSize=1000 -DisSameLocScales=0.001,0.01,0.1  -Dnum_topLocations=20 -DbinsForGTSize=0,1,2,5,10,20,50 -DrangForBinsOfGeoRedun=0,500000,100 -DaccumLevel=1,2,3,5,10,20 -DsaveRes=false -DmakeReport=true _Vis_noGlobFilter_SameUser_no1U1P _DSanFran_SURFHD20-12-20_1vs1AndHistAndAngle@0.52@0.2-1000 ImageR/BenchMark/SanFrancisco/ranks/R_SanFran_20K-VW_SURF_iniR-BurstIntraInter_HDs20-HMW12_ReR1K_HDr20_top1K_1vs1AndHistAndAngle@0.52@0.2@0@0@0@0@0@0@0_rankDocScore/data ImageR/BenchMark/SanFrancisco/GVR/ 700
	 * ME14:	hadoop jar MapRed_GeoVisualRanking.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,JSAT_my.jar,lire136.jar -Dlatlons=MediaEval14/MEval14_photos_latlons.floatArr -DuserIDs_0=MediaEval14/MEval14_photos_userIDs_0.long -DuserIDs_1=MediaEval14/MEval14_photos_userIDs_1.int -DG_ForGTSize=0.01 -DV_ForGTSize=1000 -DisSameLocScales=0.001,0.01,0.1  -Dnum_topLocations=20 -DbinsForGTSize=0,1,2,5,10,20 -DrangForBinsOfGeoRedun=0,5000,5 -DaccumLevel=1,2,3,5,10,20 -DsaveRes=false -DmakeReport=true _Vis_noGlobFilter_noSameUser_1U1P _D-ME14Pho5M_Q-Pho500K_SURFHD18-20-18_1vs1AndHPM6-1000 MediaEval14/ranks/R_MEva14_5MPho_20K-VW_SURF_HDs18-HMW20_ReR1K_HDr18_top1K_1vs1AndHPM@4@6_rankDocScore MediaEval14/GVR/ 700
	 * ME14:	hadoop jar MapRed_GeoVisualRanking.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,JSAT_my.jar,lire136.jar -Dlatlons=MediaEval14/ME14_Pho-TrTe_VidFrame-Tr_latlons.floatArr -DuserIDs_0=MediaEval14/ME14_Pho-TrTe_VidFrame-Tr_userIDs_0.long -DuserIDs_1=MediaEval14/ME14_Pho-TrTe_VidFrame-Tr_userIDs_1.int -DG_ForGTSize=0.01 -DV_ForGTSize=1000 -DisSameLocScales=0.001,0.01,0.1  -Dnum_topLocations=20 -DbinsForGTSize=0,1,2,5,10,20 -DrangForBinsOfGeoRedun=0,5000,5 -DaccumLevel=1,2,3,5,10,20 -DsaveRes=false -DmakeReport=true _Vis_noGlobFilter_noSameUser_1U1P _D-ME14Pho5MFra1M_Q0-Pho500K_SURFHD18-20-18_1vs1AndHPM6-1000 MediaEval14/ranks/R_MEva14_5MPho1MFra_20K-VW_SURF_HDs18-HMW20_ReR1K_HDr18_top1K_1vs1AndHPM@4@6_Q0_25_R0-0_H0-0_rankDocScore MediaEval14/GVR/ 700
	 */
	
	public static final String hdfs_address="hdfs://head02.hathi.surfsara.nl/user/yliu/"; //hdfs://p-head03.alley.sara.nl/, hdfs://head02.hathi.surfsara.nl/

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_GeoVisualRanking(), args);
		System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem hdfs=FileSystem.get(conf);
		String[] otherArgs = args; //use this to parse args!
		String dateFormate="yyyy.MM.dd G 'at' HH:mm:ss z";
		//set common
		String taskLabel;     
		boolean saveRes=Boolean.valueOf(conf.get("saveRes"));
		boolean makeReport=Boolean.valueOf(conf.get("makeReport"));
		
		String jobLabel=otherArgs[0];//_Vis or _RankScore or _Num, _GlobFilter-Gist (_GlobFilter-Lire@JCD) or _noGlobFilter, _sameUser or _noSameUser, _1U1P or _no1U1P, _Norm or _noNorm
		String baseRankLabel=otherArgs[1];//_D9M_Q250K_SURFHD18-20-18_1vs1AndHPM6-1000,  _D9M_Q250K_SURFHD18-20-18_1vs1AndHPM6-1000_Matches
		String baseRankPath=otherArgs[2];//TMM_GVR/imagR/ranks/SURF_D9M_Q250K_Rank_HDs12-HMW20_ReR1000_HDr20_top1K_1vs1AndHPM6_Q0_8
		String workDir=otherArgs[3];//TMM_GVR/GVR/
		int RedNumForProcRank=Integer.valueOf(otherArgs[4]);//700
		
		PrintWriter outStr_report=null;
		if (makeReport) {
			outStr_report=new PrintWriter(new OutputStreamWriter(hdfs.create(new Path(hdfs_address+workDir+"Report"+jobLabel+baseRankLabel),false), "UTF-8"),true); 
		}
		
		General.dispInfo(outStr_report,"...............baseRankLabel:"+baseRankLabel+", jobLabel:"+jobLabel+"   ............. ............. ");
		General.dispInfo(outStr_report,"num_topLocations:"+conf.get("num_topLocations")+", sameLoc: "+conf.get("isSameLocScales"));
		General.dispInfo(outStr_report,General.dispTimeDate(System.currentTimeMillis(), dateFormate)+", start processing!  ..................");
			
		String geoRedunBinInfoPath=hdfs_address+workDir+"geoRedunBinInfo"+jobLabel+baseRankLabel+".InfoStr";

		Path[] visRankPaths=General_Hadoop.strArr_to_PathArr(baseRankPath.split(","));
		
		//******* run for reRank***********	
		//parameters for GlobFilter
		int[] RankThrs= jobLabel.contains("_GlobFilter")? new int[]{100}:new int[]{0}; //10,50,100
		double[] RankScales=jobLabel.contains("_GlobFilter")? new double[]{3}:new double[]{0}; //2,3
		//parameters for GVR
		int[] reRankScales_GVR={10,20}; //10,20,50,100
		int[] visScales_GVR={100}; //10,20,50,100
		double[] geoExpanScales_GVR={0.001,0.01};//0.001,0.01,0.1
		//parameters for MeanShift
		int[] MeanShift_reRankScales={10,20}; //10,20,50,100
		double[] MeanShift_bandSca={0.0001}; //0.0001,0.00001
		int MeanShift_maxInter=100;
		//parameters for GMR
		int[] reRankScales_GMR={10,20,50}; //10,20,50,100
		int[] visScales_GMR={50,100}; //10,20,50,100
		double[] geoExpanScales_GMR={0.001,0.01};//0.001,0.01,0.1
		
		for (int RankThr:RankThrs){ // loop over parameters.
			for (double RankScale:RankScales){
				String globFilterLabel=jobLabel.contains("_GlobFilter")?"_RankThr@"+RankThr+"_RankSca@"+RankScale:"";
				//******** VisNN ************
				taskLabel="_VisNN"+jobLabel+globFilterLabel;
				oneJobsLoop(taskLabel, baseRankLabel, saveRes,  conf,  hdfs, hdfs_address, outStr_report, visRankPaths, workDir,RedNumForProcRank,geoRedunBinInfoPath);
				//******** GVR ************
				for (int reRankScale:reRankScales_GVR){
					for (int visScale:visScales_GVR){ // loop over parameters
						if ((reRankScale<=visScale) && (RankThr==0 || RankThr>=visScale)) {
							for(double geoExpanScale:geoExpanScales_GVR){
								//*********  run jobs loop ***************
								taskLabel="_GVR"+jobLabel+globFilterLabel+"_reRank@"+reRankScale+"_visSca@"+visScale+"_expSca@"+geoExpanScale;//
								oneJobsLoop(taskLabel, baseRankLabel, saveRes,  conf,  hdfs, hdfs_address, outStr_report, visRankPaths, workDir,RedNumForProcRank,geoRedunBinInfoPath);
							}
						}
					}
				}
				//******** MeanShiftClustering ************
				for (int reRankScale:MeanShift_reRankScales){
					for(double bandSca:MeanShift_bandSca){
						//*********  run jobs loop ***************
						taskLabel="_MeanShift"+jobLabel+globFilterLabel+"_reRank@"+reRankScale+"_maxI@"+MeanShift_maxInter+"_bandSca@"+bandSca;//_reRank@100_maxI@100_bandSca@0.001
						oneJobsLoop(taskLabel, baseRankLabel, saveRes,  conf,  hdfs, hdfs_address, outStr_report, visRankPaths, workDir,RedNumForProcRank,geoRedunBinInfoPath);
					}
				}
				//******** GMR ************
				for (int reRankScale:reRankScales_GMR){
					for (int visScale:visScales_GMR){ // loop over parameters
						if ((reRankScale<=visScale) && (RankThr==0 || RankThr>=visScale)) {
							for(double geoExpanScale:geoExpanScales_GMR){
								//*********  run jobs loop ***************
								taskLabel="_GMR"+jobLabel+globFilterLabel+"_reRank@"+reRankScale+"_visSca@"+visScale+"_expSca@"+geoExpanScale;//
								oneJobsLoop(taskLabel, baseRankLabel, saveRes,  conf,  hdfs, hdfs_address, outStr_report, visRankPaths, workDir,RedNumForProcRank,geoRedunBinInfoPath);
							}
						}
					}
				}
			}
		}
		General.closePrintWriterOnExist(outStr_report);
		General_Hadoop.deleteIfExist(geoRedunBinInfoPath, hdfs);
		hdfs.close();
		return 0;
	}
	
	public void oneJobsLoop(String taskLabel, String baseRankLabel, boolean saveRes, Configuration conf, FileSystem hdfs, 
			String homePath, PrintWriter outStr_report, Path[] In, String Out, int RedNumForProcRank, String geoRedunBinInfoPath) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException{
		//set label
		conf.set("taskLabel",taskLabel);
		General.dispInfo(outStr_report,"\n .......task label:"+taskLabel);
		//set commons
		String globalReranked=null;
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		String out_reranked=Out+"rank"+taskLabel+baseRankLabel; //output path for reranked ranks

		//******* 1st job: query rerank  ******
		//set input/output path
		String job1_out=out_reranked+"_tempJob1Out";
		//set distributed cache, add latlons and concept Mapfile to Distributed cache
		cacheFilePaths.clear();
		cacheFilePaths.add(homePath+conf.get("latlons")+"#latlons.file"); //latlons path with symLink
		if(taskLabel.contains("_GlobFilter")){
			globalReranked= GlobalRerank(taskLabel, baseRankLabel, conf, hdfs, homePath, In, Out, RedNumForProcRank);
			cacheFilePaths.add(homePath+globalReranked+"/part-r-00000/data#data"); //globalReranked_MapFile data path with symLink
			cacheFilePaths.add(homePath+globalReranked+"/part-r-00000/index#index"); //globalReranked_MapFile data path with symLink
		}
		if(taskLabel.contains("_noSameUser")||taskLabel.contains("_1U1P")){
			cacheFilePaths.add(homePath+conf.get("userIDs_0")+"#userIDs_0.file"); //userIDs_0
			cacheFilePaths.add(homePath+conf.get("userIDs_1")+"#userIDs_1.file"); //userIDs_1
		}
		//set reducer number
		int job1RedNum=RedNumForProcRank; //reducer number
		//run
		General_Hadoop.Job(conf, In, job1_out, taskLabel, job1RedNum, 8, 2, true,
				MapRed_GeoVisualRanking.class, null, Partitioner_random.class,null,null,Reducer_processRankDocScore.class,
				IntWritable.class, IntList_FloatList.class, IntWritable.class, fistMatch_GTruth_Docs_Locations.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		
		//******* 2nd job: save, combine querys result ******
		if (saveRes) {
			//------------- combine querys result -----------------//
			General_Hadoop.Job(conf, new Path[]{new Path(job1_out)}, out_reranked, "save", 1, 8, 2, true,
					MapRed_GeoVisualRanking.class, null, null,null,null,Reducer_InOut_1key_1value.class,
					IntWritable.class, fistMatch_GTruth_Docs_Locations.class, IntWritable.class,fistMatch_GTruth_Docs_Locations.class,
					SequenceFileInputFormat.class, MapFileOutputFormat.class, 0L, 10,
					null,null);
		}
		//******* 3nd job: analysis querys result******
		if (outStr_report!=null) {
			//run getEquBinForGeoRedundency
			if (!hdfs.exists(new Path(geoRedunBinInfoPath))) {//this is the first time to make geoRedunBinInfo
				conf.set("InfoStrPath",geoRedunBinInfoPath); // save String object to InfoStrPath
				General_Hadoop.Job(conf, new Path[]{new Path(job1_out)}, null, "getEquBin"+taskLabel, 1, 8, 2, false,
						MapRed_GeoVisualRanking.class, null, null,null,null,Reducer_getEquBinForGeoRedundency.class,
						IntWritable.class, fistMatch_GTruth_Docs_Locations.class, IntWritable.class,fistMatch_GTruth_Docs_Locations.class,
						SequenceFileInputFormat.class, NullOutputFormat.class, 0L, 0,
						null,null);
			}
			String Info=(String) General_Hadoop.readObject_HDFS(hdfs, geoRedunBinInfoPath);
			conf.set("binsForGeoRedun",Info); //for Job3		
			//set info
			String InfoStrPath=homePath+out_reranked+".InfoStr";
			conf.set("InfoStrPath",InfoStrPath); //Job3 save MAPInfo as String object to InfoStrPath
			//set distributed cache, add latlons to Distributed cache
			cacheFilePaths.clear();
			cacheFilePaths.add(homePath+conf.get("latlons")+"#latlons.file"); //latlons path with symLink
			//run
			General_Hadoop.Job(conf, new Path[]{new Path(job1_out)}, null, "Analysis"+taskLabel, 1, 8, 2, true,
					MapRed_GeoVisualRanking.class, null, null,null,null,Reducer_makeReport.class,
					IntWritable.class, fistMatch_GTruth_Docs_Locations.class, IntWritable.class,fistMatch_GTruth_Docs_Locations.class,
					SequenceFileInputFormat.class, NullOutputFormat.class, 0L, 0,
					cacheFilePaths.toArray(new String[0]),null);
			Info=(String) General_Hadoop.readObject_HDFS(hdfs, InfoStrPath);
			General.dispInfo(outStr_report,Info);
			outStr_report.flush();	
			//********* clean-up ***********//
			hdfs.delete(new Path(InfoStrPath), true);
		}
		//********* clean-up ***********//
		if(globalReranked!=null){
			hdfs.delete(new Path(globalReranked), true);
		}
		hdfs.delete(new Path(job1_out), true);
		General.dispInfo(outStr_report,General.dispTimeDate(System.currentTimeMillis(), "yyyy.MM.dd G 'at' HH:mm:ss z")+", finished!  ");	
	}
	
	public String GlobalRerank(String taskLabel, String baseRankLabel, Configuration conf, FileSystem hdfs, String homePath, Path[] locFeatRankPath, String wordir, int reducerNum) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException{
		//set commons
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		String out_GlobalReranked=wordir+"GlobalRerank"+taskLabel+baseRankLabel; //output path for reranked ranks
		//******* 1st job: select top docs from local-rank  ******
		//run
		String job1_1_out=out_GlobalReranked+"_tempJob1_2Out_selTopRank";
		General_Hadoop.Job(conf, locFeatRankPath, job1_1_out, "GlobalRerank_preSelDoc", 1, 8, 2, true,
				MapRed_GeoVisualRanking.class, Mapper_selectTopRank.class, null, null,null, Reducer_InOut_1key_1value.class,
				IntWritable.class, IntArr.class, IntWritable.class,IntArr.class,
				SequenceFileInputFormat.class, MapFileOutputFormat.class, 0L, 0,
				null,null);
		//run
		String job1_2_out=out_GlobalReranked+"_tempJob1_1Out_docID_queryIDs";
		General_Hadoop.Job(conf, locFeatRankPath, job1_2_out, "GlobalRerank_preSelDoc", 1, 8, 2, true,
				MapRed_GeoVisualRanking.class, Mapper_selectTopRank_transfer.class, null, null,null, Reducer_makeDocID_QueryIDs.class,
				IntWritable.class, IntWritable.class, IntWritable.class,IntArr.class,
				SequenceFileInputFormat.class, MapFileOutputFormat.class, 0L, 0,
				null,null);
		
		//******* 2st job: do global-filtering  ******
		String globalFeatPath=conf.get("FilterMapFile")+"/part-r-00000/data";
		String job2_1_out=out_GlobalReranked+"_tempJob2_1Out";
		String job2_2_out=out_GlobalReranked+"_tempJob2_2Out";
		if (taskLabel.contains("Lire")) {//_GlobFilter-Lire@JCD
			//run
			cacheFilePaths.clear();
			cacheFilePaths.add(homePath+job1_2_out+"/part-r-00000/data#data"); //docID_querIDs.data
			cacheFilePaths.add(homePath+job1_2_out+"/part-r-00000/index#index"); //docID_querIDs.index
			General_Hadoop.Job(conf, new Path[]{new Path(globalFeatPath)}, job2_1_out, "GlobalRerank_selFeat", reducerNum, 8, 2, true,
					MapRed_GeoVisualRanking.class, null, Partitioner_random.class, null,null, Reducer_selectFeat_Lire.class,
					IntWritable.class, BytesWritable.class, IntWritable.class,IntArr_byteArr.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0L, 0,
					cacheFilePaths.toArray(new String[0]),null);
			//run
			cacheFilePaths.clear();
			cacheFilePaths.add(homePath+job1_1_out+"/part-r-00000/data#data"); //ranks.data
			cacheFilePaths.add(homePath+job1_1_out+"/part-r-00000/index#index"); //ranks.index
			General_Hadoop.Job(conf, new Path[]{new Path(job2_1_out)}, job2_2_out, "GlobalRerank", reducerNum, 8, 2, true,
					MapRed_GeoVisualRanking.class, null, Partitioner_random_sameKey.class, null,null, Reducer_rankOnGlobal_Lire.class,
					IntWritable.class, IntArr_byteArr.class, IntWritable.class,IntArr.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0L, 0,
					cacheFilePaths.toArray(new String[0]),null);
		}else {
			//run
			cacheFilePaths.clear();
			cacheFilePaths.add(homePath+job1_2_out+"/part-r-00000/data#data"); //docID_querIDs.data
			cacheFilePaths.add(homePath+job1_2_out+"/part-r-00000/index#index"); //docID_querIDs.index
			General_Hadoop.Job(conf, new Path[]{new Path(globalFeatPath)}, job2_1_out, "GlobalRerank_selFeat", reducerNum, 8, 2, true,
					MapRed_GeoVisualRanking.class, null, Partitioner_random.class, null,null, Reducer_selectFeat.class,
					IntWritable.class, FloatArr.class, IntWritable.class,IntArr_FloatArr.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0L, 0,
					cacheFilePaths.toArray(new String[0]),null);
			//run
			cacheFilePaths.clear();
			cacheFilePaths.add(homePath+job1_1_out+"/part-r-00000/data#data"); //ranks.data
			cacheFilePaths.add(homePath+job1_1_out+"/part-r-00000/index#index"); //ranks.index
			General_Hadoop.Job(conf, new Path[]{new Path(job2_1_out)}, job2_2_out, "GlobalRerank", reducerNum, 8, 2, true,
					MapRed_GeoVisualRanking.class, null, Partitioner_random_sameKey.class, null,null, Reducer_rankOnGlobal.class,
					IntWritable.class, IntArr_FloatArr.class, IntWritable.class,IntArr.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0L, 0,
					cacheFilePaths.toArray(new String[0]),null);
		}
		General_Hadoop.Job(conf, new Path[]{new Path(job2_2_out)}, out_GlobalReranked, "combine", 1, 8, 2, true,
				MapRed_GeoVisualRanking.class, null, null, null,null, Reducer_InOut_1key_1value.class,
				IntWritable.class, IntArr.class, IntWritable.class,IntArr.class,
				SequenceFileInputFormat.class, MapFileOutputFormat.class, 0L, 0,
				null,null);
		
		//clean-up
		hdfs.delete(new Path(job1_1_out), true);
		hdfs.delete(new Path(job1_2_out), true);
		hdfs.delete(new Path(job2_1_out), true);
		hdfs.delete(new Path(job2_2_out), true);
		
		return out_GlobalReranked;
	}
	
	//******* 1st job: query rerank  ******
	public static class Reducer_processRankDocScore extends Reducer<IntWritable,IntList_FloatList,IntWritable,fistMatch_GTruth_Docs_Locations>  {

		//for user
		private boolean isNoSameUser;
		private boolean is1U1P;
		private long[] userIDs_0;
		private int[] userIDs_1;
		//for calculating groundTruth
		private float G_ForGTSize;
		private int V_ForGTSize;
		private float[][] latlons;
		//for Global-Filter
		private boolean isGlobalFilter;
		private MapFile.Reader globalReranked_MapFile;
		private int GlobalFilter_RankThr;
		
		//which sim to use
		private boolean isUseVisSim;
		private boolean isUseRankingScore;
		private boolean isUseNum;
		private boolean isNormal;
		//VisNN
		private boolean isVisNN;
		//GVR
		private boolean isGVR;
		private int GVR_reRankScale;
		private int GVR_topVisScale;
		private float GVR_geoExpanSca;
		//MeanShift
		private boolean isMeanShift;
		private int MShift_reRankScale;
		private int MShift_maxIteration;
		private double MShift_BandScale;
		//common
		private int queryNum;
		private int num_topLocations;
		private boolean disp;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			isNoSameUser=false; isGlobalFilter=false; isVisNN=false; isGVR=false; isMeanShift=false;
			isUseVisSim=false; isUseRankingScore=false; isUseNum=false; 
			isNormal=false; 
			//******* read taskLabel**************
			String taskLabel=conf.get("taskLabel");
			System.out.println("taskLabel: "+taskLabel);
			//******* read G_ForGTSize, V_ForGTSize**************
			G_ForGTSize=Float.valueOf(conf.get("G_ForGTSize"));
			V_ForGTSize=Integer.valueOf(conf.get("V_ForGTSize"));
			System.out.println("G_ForGTSize: "+G_ForGTSize+", V_ForGTSize:"+V_ForGTSize);
			//check file in distributted cache
			General.checkDir(new Disp(true,"",null), ".");
			latlons=(float[][]) General.readObject("latlons.file");
			userIDs_0=(long[]) General.readObject("userIDs_0.file"); 
			userIDs_1=(int[]) General.readObject("userIDs_1.file"); 
			//for user
			if (taskLabel.contains("_noSameUser")) {
				isNoSameUser=true;
				General.Assert(userIDs_0!=null && userIDs_1!=null, "this task is for no-SameUser, userIDs_0 and userIDs_1 should be both no-null!");
			}
			is1U1P=taskLabel.contains("_1U1P")?true:false; 	//check 1U1P: 1 user only contribute 1 photo in the rank list
			//****** load FilterMapFile ************
			if(taskLabel.contains("_GlobFilter")){
				isGlobalFilter=true;
				globalReranked_MapFile=General_Hadoop.openMapFileInNode("data", conf, true);
				//**** load GlobalFilter_RankThr, GlobalFilter_RankScale ************//
				for (String one:taskLabel.split("_")) {//_RankThr@10_RankSca@5
					if (one.contains("RankThr")) {
						GlobalFilter_RankThr=Integer.valueOf(one.split("@")[1]);
					}
				}
				System.out.println("GlobalFilter_RankThr: "+GlobalFilter_RankThr);
			}
			//******* read parameters for which sim to use  **************
			if (taskLabel.contains("_Vis")) {//use visual score
				isUseVisSim=true;
			}else if (taskLabel.contains("_RankScore")) {//use rank score
				isUseRankingScore=true;
			}else if (taskLabel.contains("_Num")) {//use num
				isUseNum=true;
			}
			//******* read parameters for whether norm or not  **************
			if (taskLabel.contains("_Norm")) {
				isNormal=true;
				System.out.println("isNormal: "+isNormal);
			}
			//******* read parameters used in visNN  **************
			if (taskLabel.contains("VisNN")) {//visNN method
				isVisNN=true;
			}
			//******* read parameters used in GVR  **************
			if(taskLabel.contains("_GVR")){//_reRank@100_visSca@100_expSca@0.01
				isGVR=true;
				for (String one:taskLabel.split("_")) {
					if (one.contains("reRank")) {
						GVR_reRankScale=Integer.valueOf(one.split("@")[1]);
					}else if(one.contains("visSca")){
						GVR_topVisScale=Integer.valueOf(one.split("@")[1]);
					}else if(one.contains("expSca")){
						GVR_geoExpanSca=Float.valueOf(one.split("@")[1]);
					}
				}
				System.out.println("GVR_reRankScale: "+GVR_reRankScale+", GVR_topVisScale:"+GVR_topVisScale+", GVR_geoExpanSca:"+GVR_geoExpanSca);	
			}
			//******* read parameters used in MeanShift  **************
			if(taskLabel.contains("_MeanShift")){//_reRank@100_maxI@100_bandSca@0.001
				isMeanShift=true;
				for (String one:taskLabel.split("_")) {
					if (one.contains("reRank")) {
						MShift_reRankScale=Integer.valueOf(one.split("@")[1]);
					}else if(one.contains("maxI")){
						MShift_maxIteration=Integer.valueOf(one.split("@")[1]);
					}else if(one.contains("bandSca")){
						MShift_BandScale=Double.valueOf(one.split("@")[1]);
					}
				}
				System.out.println("MShift_reRankScale: "+MShift_reRankScale+", MShift_maxIteration:"+MShift_maxIteration+", MShift_BandScale:"+MShift_BandScale);	
			}
			//******* read num_topLocations  **************
			num_topLocations=Integer.valueOf(conf.get("num_topLocations"));
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			disp=true;
			queryNum=0; //total point number in one reducer
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable QueryName, Iterable<IntList_FloatList> docs_scores_I, Context context) throws IOException, InterruptedException {
			//docs_scores: ranked docNames and Scores
			int queryName=QueryName.get(); IntList_FloatList docs_scores=new IntList_FloatList(); int loopNum=0;
			for(Iterator<IntList_FloatList> it=docs_scores_I.iterator();it.hasNext();){//only one element
				docs_scores=it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_processRank! one photoName, one ranklist, loopNum should == 1, here loopNum="+loopNum);
			if (docs_scores.getIntegers().size()== docs_scores.getFloats().size()+1) {//in global feat, last doc is the feature ind! 
				docs_scores.getIntegers().remove(docs_scores.getIntegers().size()-1);
			}
			General.Assert(docs_scores.getIntegers().size()== docs_scores.getFloats().size(), 
					"err in Reducer_processRank! docs and scores are not equal length! docs:"+docs_scores.getIntegers().size()+", scores:"+docs_scores.getFloats().size());
			//check same user and delete query itself
			if (isNoSameUser) {//need delete same user, so query itself also deleted!
				General_geoRank.removeSameUser_forTopDocsScores(docs_scores.getIntegers(), docs_scores.getFloats(), queryName, userIDs_0, userIDs_1);
			}else {//no need to delete same user, but only query itself should be deleted!
				General_geoRank.removeQueryItself_forTopDocsScores(docs_scores.getIntegers(), docs_scores.getFloats(), queryName);
			}
			int totRank_length=docs_scores.getIntegers().size();
			//calculate groundTruth
			ArrayList<Integer> visNeig_ForGTSize=new ArrayList<Integer>(docs_scores.getIntegers().subList(0, Math.min(totRank_length, V_ForGTSize)));
			ArrayList<int[]> gTruth= General_geoRank.get_GTruth(queryName, G_ForGTSize,visNeig_ForGTSize, latlons); //ArrayList<rank_photoName>
			if (gTruth.size()==0) {//no ground truth for this query, mark this use {-1,-1}
				int[] cell={-1,-1};
				gTruth.add(cell);
			}
			//calculate geoReduncy and saveInto gTruth
			int geoReduncy=General_geoRank.findGeoNeighbors(queryName, G_ForGTSize, latlons).size();//geo-neighbor num
			//apply globalFilter
			if(isGlobalFilter){
				HashSet<Integer> selDocs=null;
				selDocs=General_geoRank.get_GoodDoc_basedOn_globalFeatRank(globalReranked_MapFile, queryName, GlobalFilter_RankThr,docs_scores.getIntegers());
				//pick out good docs
				General_geoRank.selGoodRankDocs(docs_scores.getIntegers(), docs_scores.getFloats(), selDocs);
				//update totRank_length
				totRank_length=docs_scores.getIntegers().size();
			}
			//***********  apply visNN, GVR, 
			GTruth fistMatch = null; ArrayList<ArrayList<DID_Score>> topLocationDocs = null; ArrayList<float[]> topLocations=null;
			if (isVisNN) {//visNN method
				//make topLocations
				int actNum_topLocations=Math.min(num_topLocations,totRank_length);//some query do not have enough listed docs
				topLocationDocs=new ArrayList<ArrayList<DID_Score>>(actNum_topLocations); topLocations=new ArrayList<float[]>(actNum_topLocations);
				for(int i=0;i<actNum_topLocations;i++){
					int docID=docs_scores.getIntegers().get(i);
					ArrayList<DID_Score> oneloc=new ArrayList<DID_Score>();
					oneloc.add(new DID_Score(docID, docs_scores.getFloats().get(i)));
					topLocationDocs.add(oneloc);
					topLocations.add(new float[]{latlons[0][docID], latlons[1][docID]});
				}
			}else if (isGVR) {//GVR
				//**** 1.a use GVR_topVisScale to only work with top-ranked docs
				int actVisScale=Math.min(GVR_topVisScale,totRank_length);//some query do not have enough listed docs
				//transfer docScore to similarity, rankingScore or num
				ArrayList<Integer> docs_needed=new ArrayList<Integer>(docs_scores.getIntegers().subList(0, actVisScale));
				ArrayList<Float> docScores_needed=General_geoRank.transferDocScore(docs_scores.getFloats().subList(0, actVisScale), isUseVisSim, isUseRankingScore, isUseNum);
				//make locations
				ArrayList<ArrayList<DID_Score>> LocationDocs=new ArrayList<ArrayList<DID_Score>>(GVR_reRankScale); ArrayList<float[]> Locations=new ArrayList<float[]>(GVR_reRankScale);
				General_geoRank.get_topLocationDocsList(LocationDocs, Locations, GVR_reRankScale, docs_needed, docScores_needed, GVR_geoExpanSca*2, latlons, is1U1P, userIDs_0, userIDs_1);
//				//**** 1.b use GVR_topVisScale to control photoNumPerLoc
//				//transfer docScore to similarity, rankingScore or num
//				ArrayList<Integer> docs_needed=docs_scores.getIntegers();
//				ArrayList<Float> docScores_needed=General_geoRank.transferDocScore(docs_scores.getFloats(), isUseVisSim, isUseRankingScore, isUseNum);
//				//make locations
//				ArrayList<ArrayList<DID_Score>> LocationDocs=new ArrayList<ArrayList<DID_Score>>(GVR_reRankScale); ArrayList<float[]> Locations=new ArrayList<float[]>(GVR_reRankScale);
//				General_geoRank.get_topLocationDocsList_tunePhoNumPerLoc(LocationDocs, Locations, GVR_reRankScale, docs_needed, docScores_needed, GVR_geoExpanSca*2, GVR_topVisScale, latlons, is1U1P, userIDs_0, userIDs_1);
				//**** 2 rerank location
				topLocationDocs=new ArrayList<ArrayList<DID_Score>>(GVR_reRankScale); topLocations=new ArrayList<float[]>(GVR_reRankScale);
				General_geoRank.rerank_locations_byScore(LocationDocs, Locations, num_topLocations, topLocationDocs, topLocations, isNormal);
			}else if (isMeanShift) {//MeanShiftClustering
				int reRank_length=Math.min(MShift_reRankScale,totRank_length);//some query do not have enough listed docs
				//make oriRanks
				ArrayList<Integer> oriRanks=new  ArrayList<Integer>(docs_scores.getIntegers().subList(0, reRank_length)); //use all
				ArrayList<Float> docScores_needed=General_geoRank.transferDocScore(docs_scores.getFloats().subList(0, reRank_length), isUseVisSim, isUseRankingScore, isUseNum);
				//******** rerank ********//
				topLocationDocs=new ArrayList<ArrayList<DID_Score>>(num_topLocations); topLocations=new ArrayList<float[]>(num_topLocations);
				General_geoRank.rerank_meanShift_retrunRankScore(oriRanks, docScores_needed, topLocationDocs, topLocations, num_topLocations, latlons[0], latlons[1], MShift_BandScale, MShift_maxIteration, is1U1P, userIDs_0, userIDs_1);
			}
			//find rank of the first true math
			fistMatch = GTruth.get_firstTrueMatch(queryName, topLocationDocs, topLocations, G_ForGTSize, latlons);
			
			//**output
			fistMatch_GTruth_Docs_Locations outValue=new fistMatch_GTruth_Docs_Locations(fistMatch,gTruth,topLocationDocs,topLocations,geoReduncy);
			context.write(QueryName, outValue);
			if(disp==true){
				System.out.println("QueryName:"+QueryName.get()+", visual rank-length:"+totRank_length);
				System.out.println("outValue.fistMatch:"+outValue.fistMatch.getIntArr()[0]+", "+outValue.fistMatch.getIntArr()[1]);
				System.out.println("outValue.groudTSize:"+outValue.gTruth.getArrArr().length);
				System.out.println("outValue.Docs.length(==topLocations.length):"+outValue.Docs.getArr().length+", [0-th loc, 0-th doc_score]:"+outValue.Docs.getArr()[0].getArr()[0]);
				System.out.println("outValue.topLocations.length:"+outValue.topLocations.getArrArr().length);
				disp=false;
			}		
			queryNum++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one reducer finished! total query number: "+queryNum);
			super.setup(context);
	 	}
	}
	
	//******* 2nd job: analysis query result******
	public static class Reducer_getEquBinForGeoRedundency extends Reducer<IntWritable,fistMatch_GTruth_Docs_Locations,IntWritable,fistMatch_GTruth_Docs_Locations>  {
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
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable Key_queryName, Iterable<fistMatch_GTruth_Docs_Locations> geoExpansionData, Context context) throws IOException, InterruptedException {
			//key: queryName, value: rank result
			queryNum++;
			//******** only one list in rank result! ************		
			fistMatch_GTruth_Docs_Locations oneResult = null; int loopNum=0; 
			for(Iterator<fistMatch_GTruth_Docs_Locations> it=geoExpansionData.iterator();it.hasNext();){// loop over all HashMaps				
				oneResult= it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_makeReport! one photoName, one rank, loopNum should == 1, here loopNum="+loopNum);
			
			//******* analysis this query's result by different evaluation radius *********************
			int geoReduncy=oneResult.geoDensity;//geoReduncy
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
			super.setup(context);
	 	}
	}

	//******* 3rd job: analysis query result******
	public static class Reducer_makeReport extends Reducer<IntWritable,fistMatch_GTruth_Docs_Locations,IntWritable,fistMatch_GTruth_Docs_Locations>  {
		private Configuration conf;
		private FileSystem hdfs;
		private float[] isSameLocScales;
		private int num_evalRadius;
		private int num_topLocations;
		private float[][] latlons;
		private MakeHist_GVR makeHist_forGTSize;
		private MakeHist_GVR makeHist_forGeoRedun;
		private int maxGeoRed;
		private int maxGeoRed_QueryName;
		private int maxGTSize;
		private int maxGTSize_QueryName;
		private int[] accumLevel;
		private String InfoStrPath;
		private int[][] totlocHist_reranks;
		private int queryNums;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			hdfs=FileSystem.get(conf);
			//******* read taskLabel**************
			String taskLabel=conf.get("taskLabel");
			System.out.println("taskLabel: "+taskLabel);
			//******* read isOneLocScale, num_topLocations **************
			isSameLocScales=General.StrArrToFloatArr(conf.get("isSameLocScales").split(","));// isSameLocScales, 0.01,0.1,1.0
			num_topLocations=Integer.valueOf(conf.get("num_topLocations"));
			
			//******* set num_evalRadius  **************
			num_evalRadius=isSameLocScales.length;
			
			latlons=(float[][]) General.readObject("latlons.file");
//				geoNeighNums=(int[]) General_Hadoop.readObject_HDFS(hdfs, conf.get("geoNeighNums"));
//				userIDs_0=(long[]) General_Hadoop.readObject_HDFS(hdfs, conf.get("userIDs_0"));
//				userIDs_1=(int[]) General_Hadoop.readObject_HDFS(hdfs, conf.get("userIDs_1"));
			
			//******* setup makeHist_forGTSize **************
			makeHist_forGTSize=new MakeHist_GVR(conf.get("binsForGTSize"), num_evalRadius, num_topLocations);
			
			//******* setup makeHist_forGeoRedun **************
			makeHist_forGeoRedun=new MakeHist_GVR(conf.get("binsForGeoRedun"), num_evalRadius, num_topLocations);
			
			//******* set maxGeoRed, maxGTSize
			maxGeoRed=0; maxGTSize=0; 
			
			//******* read accumLevel**************
			accumLevel=General.StrArrToIntArr(conf.get("accumLevel").split(","));// accumLevel, 1,2,3,5,10,20
			
			//**** set InfoStrPath ************//
			InfoStrPath=conf.get("InfoStrPath");
			
			//**** set totlocHist_rerank ************//
			totlocHist_reranks=new int[num_evalRadius][num_topLocations+1]; //fist one is "not in top"
			
			queryNums=0;
			// ***** setup finsihed ***//
			System.out.println("only 1 reducer, combine result and analysize performance, save String obj to InfoStrPath: "+InfoStrPath);
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable Key_queryName, Iterable<fistMatch_GTruth_Docs_Locations> geoExpansionData, Context context) throws IOException, InterruptedException {
			//key: queryName, value: rank result
			
			//******** only one list in rank result! ************		
			int queryName=Key_queryName.get(); fistMatch_GTruth_Docs_Locations oneResult = null; int loopNum=0; 
			for(Iterator<fistMatch_GTruth_Docs_Locations> it=geoExpansionData.iterator();it.hasNext();){// loop over all HashMaps				
				oneResult= it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_makeReport! one photoName, one rank, loopNum should == 1, here loopNum="+loopNum+", queryName:"+queryName);
			
			//******* analysis this query's result by different evaluation radius *********************
			int grounTSize=oneResult.gTruth.getArrArr().length; 
			if (grounTSize==1 && oneResult.gTruth.getArrArr()[0].getIntArr()[0]==-1) {//if query do not have ground truth, then in GVR, it mark this with {-1,-1}
				grounTSize=0;
			}
			int geoReduncy=oneResult.geoDensity;//geoReduncy
			for (int i = 0; i < num_evalRadius; i++) {
				float isSameLocScale=isSameLocScales[i];
				//get top Locations
				float[][] topLocations=oneResult.topLocations.getArrArr();
				//get True-Location rank
				int trueLocRank=General_geoRank.get_trueLocRank(topLocations, queryName, num_topLocations, isSameLocScale, latlons)+1;
				//set groTSize_locHist_Group
				makeHist_forGTSize.addOneSample(grounTSize, i, trueLocRank);
				//set makeHist_forGeoRedun
				makeHist_forGeoRedun.addOneSample(geoReduncy, i, trueLocRank);
				//set totlocHist
				totlocHist_reranks[i][trueLocRank]++;
			}
			//update maxGeoRed, maxGTSize
			if (maxGeoRed<geoReduncy) {
				maxGeoRed=geoReduncy;
				maxGeoRed_QueryName=queryName;
			}
			if (maxGTSize<grounTSize) {
				maxGTSize=grounTSize;
				maxGTSize_QueryName=queryName;
			}
			queryNums++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			//outPut as String
			StringBuffer outInfo=new StringBuffer();
			outInfo.append("accumLevel:"+"\t"+General.IntArrToString(accumLevel, "\t")+"\n");
			for (int i = 0; i < num_evalRadius; i++) {
				float isSameLocScale=isSameLocScales[i];
				outInfo.append("isSameLocScale:"+isSameLocScale+"\n");
				// ** compute accumulated TrueLocHist for grouped grounTSize ***//
				outInfo.append(makeHist_forGTSize.makeRes(i, accumLevel, "******** Group grounTSize:  accumulated-TrueLocHist: \n"));
				// ** compute accumulated TrueLocHist for grouped geo-redundency ***//
				outInfo.append(makeHist_forGeoRedun.makeRes(i, accumLevel, "******** Group geo-redundency:  accumulated-TrueLocHist: \n"));
				// ** compute accumulated TrueLocHist for totlocHist_ori, totlocHist_rerank ***//
				int[] totlocHistAccu_rerank=General.makeAccum(accumLevel, totlocHist_reranks[i]); //no "not in top" querys, so when compute percent, should ./ totalQueryNum
				outInfo.append("******** total querys:  accumulated-TrueLocHist: \n");
				outInfo.append(General.floatArrToString(General.normliseArr(totlocHistAccu_rerank, queryNums), "\t", "0.0000")+"\n\n");
			}
			outInfo.append("******** tot "+queryNums+" querys, maxGeoRed:"+maxGeoRed+", maxGeoRed_QueryName:"+maxGeoRed_QueryName+", "+latlons[0][maxGeoRed_QueryName]+", "+latlons[1][maxGeoRed_QueryName]
							+", maxGTSize:"+maxGTSize+", maxGTSize_QueryName:"+maxGTSize_QueryName+", "+latlons[0][maxGTSize_QueryName]+", "+latlons[1][maxGTSize_QueryName]);
			//outPut
			General_Hadoop.writeObject_HDFS(hdfs, InfoStrPath, outInfo.toString());
			
			// ***** setup finsihed ***//
			System.out.println("\n Reducer finished! total querys:"+queryNums);
			System.out.println("outInfo: \n"+ outInfo.toString());
			super.setup(context);
	 	}
	}
	
	//*******************  job: global filtering **********************
	public static class Mapper_selectTopRank extends Mapper<IntWritable,IntList_FloatList,IntWritable,IntArr>{
		//output is DocID_QueryID!
		private int topRerankNum;
		private boolean disp;
		private int procSamples;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//******* read taskLabel**************
			String taskLabel=conf.get("taskLabel");
			System.out.println("taskLabel: "+taskLabel);
			//**** load GlobalFilter_RankThr, GlobalFilter_RankScale ************//
			int GlobalFilter_RankThr = 0;  float GlobalFilter_RankScale = 0;
			for (String one:taskLabel.split("_")) {//_RankThr@10_RankSca@5
				if (one.contains("RankThr")) {
					GlobalFilter_RankThr=Integer.valueOf(one.split("@")[1]);
				}else if(one.contains("RankSca")){
					GlobalFilter_RankScale=Float.valueOf(one.split("@")[1]);
				}
			}
			System.out.println("GlobalFilter_RankThr: "+GlobalFilter_RankThr+", GlobalFilter_RankScale: "+GlobalFilter_RankScale);
			//**** load GlobalFilter_RankThr, GlobalFilter_RankScale ************//
			topRerankNum=(int) (GlobalFilter_RankThr*GlobalFilter_RankScale);
			System.out.println("top rerank num:"+topRerankNum);
			disp=true; 
			procSamples=0;
			// ***** setup finished ***//
			System.out.println("Mapper_selectTopRank setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void map(IntWritable key, IntList_FloatList value, Context context) throws IOException, InterruptedException {
			//key: photoName, value: (ranked) docNames_scores	
			if (disp==true){ //debug disp info
				System.out.println("Mapper: read and out");
				System.out.println("mapIn_Key, queryName: "+key.get());
				System.out.println("mapIn_Value, ranked doc_scores, length: "+value.getIntegers().size());
				System.out.println("mapIn_Value, ranked doc_scores, sample, 1st doc&scores: "+value.getIntegers().get(0)+"_"+value.getFloats().get(0));
				System.out.println("mapIn_Value, ranked doc_scores, sample, 2nd doc&scores: "+value.getIntegers().get(1)+"_"+value.getFloats().get(1));
				disp=false;
			}
			ArrayList<Integer> docs=value.getIntegers();
			int act_topNum=Math.min(docs.size(), topRerankNum);
			//** output, set key, value **//
			context.write(key,new IntArr(docs.subList(0, act_topNum)));
			procSamples++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples);
			super.setup(context);
	 	}
	}

	public static class Mapper_selectTopRank_transfer extends Mapper<IntWritable,IntList_FloatList,IntWritable,IntWritable>{
		//output is DocID_QueryID!
		private int topRerankNum;
		private boolean disp;
		private int procSamples;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//******* read taskLabel**************
			String taskLabel=conf.get("taskLabel");
			System.out.println("taskLabel: "+taskLabel);
			//**** load GlobalFilter_RankThr, GlobalFilter_RankScale ************//
			int GlobalFilter_RankThr = 0;  float GlobalFilter_RankScale = 0;
			for (String one:taskLabel.split("_")) {//_RankThr@10_RankSca@5
				if (one.contains("RankThr")) {
					GlobalFilter_RankThr=Integer.valueOf(one.split("@")[1]);
				}else if(one.contains("RankSca")){
					GlobalFilter_RankScale=Float.valueOf(one.split("@")[1]);
				}
			}
			System.out.println("GlobalFilter_RankThr: "+GlobalFilter_RankThr+", GlobalFilter_RankScale: "+GlobalFilter_RankScale);
			//**** load GlobalFilter_RankThr, GlobalFilter_RankScale ************//
			topRerankNum=(int) (GlobalFilter_RankThr*GlobalFilter_RankScale);
			System.out.println("top rerank num:"+topRerankNum);
			disp=true; 
			procSamples=0;
			// ***** setup finished ***//
			System.out.println("Mapper_selectTopRank setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void map(IntWritable key, IntList_FloatList value, Context context) throws IOException, InterruptedException {
			//key: photoName, value: (ranked) docNames_scores	
			if (disp==true){ //debug disp info
				System.out.println("Mapper: read and out");
				System.out.println("mapIn_Key, queryName: "+key.get());
				System.out.println("mapIn_Value, ranked doc_scores, length: "+value.getIntegers().size());
				System.out.println("mapIn_Value, ranked doc_scores, sample, 1st doc&scores: "+value.getIntegers().get(0)+"_"+value.getFloats().get(0));
				System.out.println("mapIn_Value, ranked doc_scores, sample, 2nd doc&scores: "+value.getIntegers().get(1)+"_"+value.getFloats().get(1));
				disp=false;
			}
			ArrayList<Integer> docs=value.getIntegers();
			int act_topNum=Math.min(docs.size(), topRerankNum);
			for (int i = 0; i < act_topNum; i++) {
				//** output, set key, value **//
				context.write(new IntWritable(docs.get(i)),key); //transfer to docID_QueryID
			}
			procSamples++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples);
			super.setup(context);
	 	}
	}

	public static class Reducer_makeDocID_QueryIDs extends Reducer<IntWritable,IntWritable,IntWritable,IntArr>  {
		private int sampleNums;
		private int reduceNum;
		private int dispInter;
		private long startTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			sampleNums=0;
			reduceNum=0;
			dispInter=100;
			startTime=System.currentTimeMillis();
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable DocID, Iterable<IntWritable> QueryIDs, Context context) throws IOException, InterruptedException {
			//key: docID, value: queryIDs
			LinkedList<Integer> queryIDs=new LinkedList<Integer>();
			for(Iterator<IntWritable> it=QueryIDs.iterator();it.hasNext();){// loop over		
				IntWritable oneSample=it.next();
				queryIDs.add(oneSample.get());
				sampleNums++;
			}
			context.write(DocID, new IntArr(queryIDs));
			
			//disp progress
			reduceNum++;
			
			if (reduceNum%dispInter==0){ //debug disp info
				System.out.println();
				System.out.println(reduceNum+" DocID finsihed! current sampleNums(queryID num):"+sampleNums+" ..."+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("finished! total sampleNums(queryID num):"+sampleNums+", reduceNum(DocID Num):"+reduceNum);
			System.out.println("time: "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			super.setup(context);
	 	}
	}
	
	//for float[] feat
	public static class Reducer_selectFeat extends Reducer<IntWritable,FloatArr,IntWritable,IntArr_FloatArr>  {
		private HashSet<Integer> queryIDs;
		private MapFile.Reader docID_queryIDs_Reader;
		private int sampleNums;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//read top-docs, make queryIDs, docID_querys
			System.out.println("before make queryIDs set, memory:"+General.memoryInfo());
			docID_queryIDs_Reader=General_Hadoop.openMapFileInNode("data", conf, true);
			IntWritable key=new IntWritable(); IntArr value=new IntArr();
			queryIDs=new HashSet<Integer>(); 
			while (docID_queryIDs_Reader.next(key, value)) {//key: docID, value: queryIDs
				for (int oneQ : value.getIntArr()) {
					queryIDs.add(oneQ);
				}
			}
			docID_queryIDs_Reader.close();
			System.out.println("make queryIDs set done! queryNum:"+queryIDs.size());
			System.out.println("current memory:"+General.memoryInfo());	
			sampleNums=0;
			docID_queryIDs_Reader=General_Hadoop.openMapFileInNode("data", conf, true);
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable sampleName, Iterable<FloatArr> values, Context context) throws IOException, InterruptedException {
			//key: sampleName, value: content

			//******** only one list in values! ************		
			int loopNum=0;  FloatArr oneSample=null;
			for(Iterator<FloatArr> it=values.iterator();it.hasNext();){// loop over all HashMaps				
				oneSample=it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_selectFeat! one sampleName, one value, loopNum should == 1, here loopNum="+loopNum);
			
			int photName=sampleName.get(); IntArr relQuerys=new IntArr();
			if (queryIDs.contains(photName)) {//this photo is a query
				context.write(new IntWritable(photName), new IntArr_FloatArr(new int[]{photName}, oneSample.getFloatArr()));
			}else if (docID_queryIDs_Reader.get(new IntWritable(photName), relQuerys)!=null) {//this photo is doc
				for (int oneQ : relQuerys.getIntArr()) {
					context.write(new IntWritable(oneQ), new IntArr_FloatArr(new int[]{photName}, oneSample.getFloatArr()));
				}
			}
			sampleNums++;
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			docID_queryIDs_Reader.close();
			System.out.println("finished! total sampleNums:"+sampleNums);
			super.setup(context);
	 	}
	}

	public static class Reducer_rankOnGlobal extends Reducer<IntWritable,IntArr_FloatArr,IntWritable,IntArr>  {
		MapFile.Reader rankReader;
		private int GlobalFilter_RankThr;
		private int sampleNums;
		private int noFeatDocNum;
		private int reduceNum;
		private int dispInter;
		private long startTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//******* read taskLabel**************
			String taskLabel=conf.get("taskLabel");
			System.out.println("taskLabel: "+taskLabel);
			//******* read iniRank**************
			rankReader=General_Hadoop.openMapFileInNode("data", conf, true);
			//**** load GlobalFilter_RankThr, GlobalFilter_RankScale ************//
			for (String one:taskLabel.split("_")) {//_RankThr@10_RankSca@5
				if (one.contains("RankThr")) {
					GlobalFilter_RankThr=Integer.valueOf(one.split("@")[1]);
				}
			}
			System.out.println("GlobalFilter_RankThr: "+GlobalFilter_RankThr);
			sampleNums=0;
			noFeatDocNum=0;
			reduceNum=0;
			dispInter=100;
			startTime=System.currentTimeMillis();
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable sampleName, Iterable<IntArr_FloatArr> values, Context context) throws IOException, InterruptedException {
			//key: queryID, value: queryID/docID and float[]
			int querID=sampleName.get(); float[] queryFeat=null; 
			ArrayList<Integer> docs=new ArrayList<Integer>(); ArrayList<Float> scores=new ArrayList<Float>(); 
			ArrayList<Integer> docs_withFeat=new ArrayList<Integer>(); ArrayList<float[]> feat_for_docs_withFeat=new ArrayList<float[]>();
			
			for(Iterator<IntArr_FloatArr> it=values.iterator();it.hasNext();){// loop over		
				IntArr_FloatArr oneSample=it.next();
				int phoID=oneSample.getIntArr()[0];
				float[] phoFeat=oneSample.getFloatArr();
				if (phoID==querID) {//this is query-feat
					queryFeat=oneSample.getFloatArr();
				}else {//this is doc-feat
					if (queryFeat==null) {//query-feat did not appear yet, so save this doc feat
						docs_withFeat.add(phoID);feat_for_docs_withFeat.add(phoFeat);
					}else {//query-feat is now exist, so compare query and doc feats directly, and save score.
						docs.add(phoID); scores.add(General.suqaredEuclidian(queryFeat, phoFeat));
					}
				}
				sampleNums++;
			}
			//for docs saved with feat
			if (docs_withFeat.size()!=0) {
				for (int i = 0; i < docs_withFeat.size(); i++) {
					docs.add(docs_withFeat.get(i)); scores.add(General.suqaredEuclidian(queryFeat, feat_for_docs_withFeat.get(i)));
				}
			}
			//find no-feat doc
			ArrayList<Integer> docNoFeat=new ArrayList<Integer>();
			IntArr oneRank=new IntArr(); HashSet<Integer> docHasFeat=new HashSet<Integer>(docs);
			rankReader.get(sampleName, oneRank);
			for (int doc : oneRank.getIntArr()) {
				if(!docHasFeat.contains(doc)){
					docNoFeat.add(doc);
				}
			}
			noFeatDocNum+=docNoFeat.size();
			//rank docs
			ArrayList<Integer> docs_top=new ArrayList<Integer>(); ArrayList<Float> scores_top=new ArrayList<Float>(); 
			General_IR.rank_get_TopDocScores_PriorityQueue(docs, scores, GlobalFilter_RankThr, docs_top, scores_top, "ASC", false, true);
			//add no-feat doc as "nutual docs"
			docs_top.addAll(docNoFeat);
			//output
			context.write(sampleName, new IntArr(docs_top));
			
			
			//disp progress
			reduceNum++;
			
			if (reduceNum%dispInter==0){ //debug disp info
				System.out.println();
				System.out.println(reduceNum+" querys finsihed!..."+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("finished! total sampleNums:"+sampleNums+", noFeatDocNum:"+noFeatDocNum+", reduceNum(queryNum):"+reduceNum);
			System.out.println("time: "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			super.setup(context);
	 	}
	}

	//for byte[] feat from lire
	public static class Reducer_selectFeat_Lire extends Reducer<IntWritable,BytesWritable,IntWritable,IntArr_byteArr>  {
		private HashSet<Integer> queryIDs;
		private MapFile.Reader docID_queryIDs_Reader;
		private int sampleNums;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//read top-docs, make queryIDs, docID_querys
			System.out.println("before make queryIDs set, memory:"+General.memoryInfo());
			docID_queryIDs_Reader=General_Hadoop.openMapFileInNode("data", conf, true);
			IntWritable key=new IntWritable(); IntArr value=new IntArr();
			queryIDs=new HashSet<Integer>(); 
			while (docID_queryIDs_Reader.next(key, value)) {//key: docID, value: queryIDs
				for (int oneQ : value.getIntArr()) {
					queryIDs.add(oneQ);
				}
			}
			docID_queryIDs_Reader.close();
			System.out.println("make queryIDs set done! queryNum:"+queryIDs.size());
			System.out.println("current memory:"+General.memoryInfo());	
			sampleNums=0;
			docID_queryIDs_Reader=General_Hadoop.openMapFileInNode("data", conf, true);
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable sampleName, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
			//key: sampleName, value: content

			//******** only one list in values! ************		
			int loopNum=0;  BytesWritable oneSample=null;
			for(Iterator<BytesWritable> it=values.iterator();it.hasNext();){// loop over all HashMaps				
				oneSample=it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_selectFeat! one sampleName, one value, loopNum should == 1, here loopNum="+loopNum);
			
			byte[] oneSample_byteArr=new byte[oneSample.getLength()];
			System.arraycopy(oneSample.getBytes(), 0, oneSample_byteArr, 0, oneSample.getLength()); //The data in value is only valid between 0 and getLength() - 1.!! when call getBytes() , it will return the byte[] by 1.5*ori_size,
			
			int photName=sampleName.get(); IntArr relQuerys=new IntArr();
			if (queryIDs.contains(photName)) {//this photo is a query
				context.write(new IntWritable(photName), new IntArr_byteArr(new int[]{photName}, oneSample_byteArr));
			}else if ((docID_queryIDs_Reader.get(new IntWritable(photName), relQuerys))!=null) {
				for (int oneQ : relQuerys.getIntArr()) {//this photo is doc
					context.write(new IntWritable(oneQ), new IntArr_byteArr(new int[]{photName}, oneSample_byteArr));
				}
			}
			sampleNums++;
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			docID_queryIDs_Reader.close();
			System.out.println("finished! total sampleNums:"+sampleNums);
			super.setup(context);
	 	}
	}

	public static class Reducer_rankOnGlobal_Lire extends Reducer<IntWritable,IntArr_byteArr,IntWritable,IntArr>  {
		MapFile.Reader rankReader;
		private int GlobalFilter_RankThr;
		private String targetLireFeatClassName;
		private int noFeatDocNum;
		private int sampleNums;
		private int reduceNum;
		private int dispInter;
		private long startTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//******* read taskLabel**************
			String taskLabel=conf.get("taskLabel");
			System.out.println("taskLabel: "+taskLabel);
			//******* read iniRank**************
			rankReader=General_Hadoop.openMapFileInNode("data", conf, true);
			//**** load GlobalFilter_RankThr, GlobalFilter_RankScale ************//
			for (String one:taskLabel.split("_")) {//_RankThr@10_RankSca@5
				if (one.contains("RankThr")) {
					GlobalFilter_RankThr=Integer.valueOf(one.split("@")[1]);
				}
			}
			System.out.println("GlobalFilter_RankThr: "+GlobalFilter_RankThr);			
			//**** load targetLireFeatClassName ************//
			if (taskLabel.contains("Lire")) {//_GlobFilter-Lire@JCD
				for (String one:taskLabel.split("_")) {
					if (one.contains("GlobFilter-Lire")) {
						targetLireFeatClassName=one.split("@")[1];
					}
				}
				System.out.println("GlobalRerank's feat is from Lire, targetLireFeatClassName: "+targetLireFeatClassName);
			}else {
				throw new InterruptedException("err! this reducer is Reducer_rankOnGlobal_Lire, jobLabel should contain Lire, jobLabel:"+taskLabel);
			}
			sampleNums=0;
			reduceNum=0;
			dispInter=100;
			startTime=System.currentTimeMillis();
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable sampleName, Iterable<IntArr_byteArr> values, Context context) throws IOException, InterruptedException {
			//key: queryID, value: queryID/docID and float[]
			int querID=sampleName.get(); byte[] queryFeat=null; 
			ArrayList<Integer> docs=new ArrayList<Integer>(); ArrayList<Float> scores=new ArrayList<Float>(); 
			ArrayList<Integer> docs_withFeat=new ArrayList<Integer>(); ArrayList<byte[]> feat_for_docs_withFeat=new ArrayList<byte[]>();
			
			for(Iterator<IntArr_byteArr> it=values.iterator();it.hasNext();){// loop over		
				IntArr_byteArr oneSample=it.next();
				int phoID=oneSample.getIntArr()[0];
				byte[] phoFeat=oneSample.getBytes();
				if (phoID==querID) {//this is query-feat
					queryFeat=oneSample.getBytes();
				}else {//this is doc-feat
					if (queryFeat==null) {//query-feat did not appear yet, so save this doc feat
						docs_withFeat.add(phoID);feat_for_docs_withFeat.add(phoFeat);
					}else {//query-feat is now exist, so compare query and doc feats directly, and save score.
						float dist = (float) General_Lire.getFeatDistance_lire136(queryFeat,phoFeat, targetLireFeatClassName);
						docs.add(phoID); scores.add(dist);
					}
				}
				sampleNums++;
			}
			if (docs_withFeat.size()!=0) {
				for (int i = 0; i < docs_withFeat.size(); i++) {
					float dist = (float) General_Lire.getFeatDistance_lire136(queryFeat,feat_for_docs_withFeat.get(i), targetLireFeatClassName);
					docs.add(docs_withFeat.get(i)); scores.add(dist);
				}
			}
			//find no-feat doc
			ArrayList<Integer> docNoFeat=new ArrayList<Integer>();
			IntArr oneRank=new IntArr(); HashSet<Integer> docHasFeat=new HashSet<Integer>(docs);
			rankReader.get(sampleName, oneRank);
			for (int doc : oneRank.getIntArr()) {
				if(!docHasFeat.contains(doc)){
					docNoFeat.add(doc);
				}
			}
			noFeatDocNum+=docNoFeat.size();
			//rank docs
			ArrayList<Integer> docs_top=new ArrayList<Integer>(); ArrayList<Float> scores_top=new ArrayList<Float>(); 
			General_IR.rank_get_TopDocScores_PriorityQueue(docs, scores, GlobalFilter_RankThr, docs_top, scores_top, "ASC", true, true);
			//add no-feat doc as "nutual docs"
			docs_top.addAll(docNoFeat);
			//output
			context.write(sampleName, new IntArr(docs_top));

			reduceNum++;
			if (reduceNum%dispInter==0){ //debug disp info
				System.out.println();
				System.out.println(reduceNum+" querys finsihed!..."+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("read and out finished! total sampleNums:"+sampleNums+", noFeatDocNum:"+noFeatDocNum+", reduceNum(queryNum):"+reduceNum);
			System.out.println("time: "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			super.setup(context);
	 	}
	}

}
