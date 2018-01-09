package RankStudy_MapRed;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.Geo.GVM;
import MyAPI.Geo.GVM_Evaluator;
import MyAPI.Geo.groupDocs.LocListProc;
import MyAPI.Geo.groupDocs.UserIDs;
import MyAPI.Obj.Disp;
import MyAPI.Obj.FindEqualSizedBin;
import MyAPI.Obj.Statistics;
import MyCustomedHaoop.Partitioner.Partitioner_equalAssign;
import MyCustomedHaoop.Partitioner.Partitioner_random;
import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_1key_1value;
import MyCustomedHaoop.Reducer.Reducer_combineReport;
import MyCustomedHaoop.ValueClass.ComposeWritableClassCollection.PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.GeoExpansionData.fistMatch_GTruth_Docs_Locations;

public class MapRed_GVM_old extends Configured implements Tool{

	/** 
	 * 
	 * @throws Exception 
	 * @command_example: 
	 * ME13TMM:	yarn jar MapRed_GVM.jar RankStudy_MapRed.MapRed_GVM -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,JSAT_my.jar,lire136.jar -Dlatlons=MediaEval13/MEval13_latlons.floatArr -DuserIDs_0=MediaEval13/MEval13_userIDs_0.long -DuserIDs_1=MediaEval13/MEval13_userIDs_1.int -DG_ForGTSize=0.01 -DV_ForGTSize=1000 -DisSameLocScales=0.01,0.1,1  -Dnum_topLocations=20 -DbinsForGTSize=0,1,2,5,10,20 -DrangForBinsOfGeoRedun=0,5000,5 -DaccumLevel=1,2,3,5,10,20 -DreRankScales=50,100, -DvisScales=50,100, -DgeoExpanScales=0.01, -DisNoSameUser=true, -Dis1U1P=false -DGVM_smoothFactors=50, -DGVM_blockPortions=10,20, -DsaveRes=false -DmakeReport=true _D9M_Q0_20KVW_SURFHD20-12-20_BurstIntraInter_1vs1AndHistAndAngle@0.52@0.2-1000 MM15/ImageR/ranks/Q0_MEva13_9M_20K-VW_SURF_iniR-BurstIntraInter_HDs20-HMW12_ReR1K_HDr20_top1K_1vs1AndHistAndAngle@0.52@0.2@1@0@0@0@0@0@0@0_rankDocMatches/data MM15/GVM/ 700 3
	 * SanFra:	yarn jar MapRed_GVM.jar RankStudy_MapRed.MapRed_GVM -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,JSAT_my.jar,lire136.jar -Dlatlons=ImageR/BenchMark/SanFrancisco/SanFrancisco_Q-DPCI_latlons.floatArr -DG_ForGTSize=0.001 -DV_ForGTSize=1000 -DisSameLocScales=0.001,0.01  -Dnum_topLocations=20 -DbinsForGTSize=0,1,2,5,10,20,50 -DrangForBinsOfGeoRedun=0,500000,100 -DaccumLevel=1,2,3,5,10,20 -DreRankScales=10,20, -DvisScales=100,200,500,1000, -DgeoExpanScales=0.001,0.01, -DisNoSameUser=false, -Dis1U1P=false -DsaveRes=false -DmakeReport=true _DSanFran_SURFHD20-12-20_1vs1AndHistAndAngle@0.52@0.2-1000 ImageR/BenchMark/SanFrancisco/ranks/R_SanFran_20K-VW_SURF_iniR-BurstIntraInter_HDs20-HMW12_ReR1K_HDr20_top1K_1vs1AndHistAndAngle@0.52@0.2@0@0@0@0@0@0@0_rankDocMatches/data ImageR/BenchMark/SanFrancisco/GVM/ 700
	 */
	
	public static final String hdfs_address="hdfs://head02.hathi.surfsara.nl/user/yliu/"; //hdfs://p-head03.alley.sara.nl/, hdfs://head02.hathi.surfsara.nl/
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
//		runOnHadoop(args);
		
		analysisReport();
	}
	
	public static void analysisReport() throws IOException, InterruptedException{
		BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream("O:/GVM/Reports/newSplitQ_perSet10k/Report_D9M_TestQ2_20KVW_SURFHD20-12-20_BurstIntraInter_1vs1AndHistAndAngle@0.52@0.2-1000"), "UTF-8"));
		int lineInter=19;
		String oneScheme=null; Statistics<String> stat=new Statistics<String>(300); 
		while ((oneScheme=inStr_photoMeta.readLine())!=null) {
			if (oneScheme.startsWith("_")) {//_GVM-blockInd@10@30_reRank@50_visSca@500_expSca@0.01_noSameUser_1U1P
				String oneLine=null;
				for (int i = 0; i < lineInter; i++) {
					oneLine=inStr_photoMeta.readLine();
				}
				stat.addSample(Float.valueOf(oneLine.split("\t")[0]), oneScheme);
				System.out.println(oneScheme+": "+oneLine);
			}
		}
		inStr_photoMeta.close();
		System.out.println("done! stat:"+stat.getFullStatistics("0.0000"));
	}
	
	public static void runOnHadoop(String[] args) throws Exception{
		oriArgs=args;
		int ret = ToolRunner.run(new MapRed_GVM_old(), args);
		System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		conf = getConf();
		hdfs=FileSystem.get(conf);
		String[] otherArgs = args; //use this to parse args!
		String dateFormate="yyyy.MM.dd G 'at' HH:mm:ss z";
		//set common
		saveRes=Boolean.valueOf(conf.get("saveRes"));
		boolean makeReport=Boolean.valueOf(conf.get("makeReport"));
		
		baseRankLabel=otherArgs[0];//_D9M_20KVW_SURFHD20-12-20_BurstIntraInter_1vs1AndHistAndAngle@0.52@0.2-1000
		visRankPath=otherArgs[1];//MM15/ImageR/ranks/Q0_MEva13_9M_20K-VW_SURF_iniR-BurstIntraInter_HDs20-HMW12_ReR1K_HDr20_top1K_1vs1AndHistAndAngle@0.52@0.2@1@0@0@0@0@0@0@0_rankDocMatches
		workDir=otherArgs[2];//MM15/GVM/
//		int redNum_style1=Integer.valueOf(otherArgs[3]);//700, for paralise process queries
		int reducerInter_style2=Integer.valueOf(otherArgs[4]);//3, for paralise process schemes
		//report
		outStr_report=null;
		if (makeReport) {
			outStr_report=new PrintWriter(new OutputStreamWriter(hdfs.create(new Path(hdfs_address+workDir+"Report"+baseRankLabel),false), "UTF-8"),true); 
		}
		General.dispInfo(outStr_report,"oriArgs:"+General.StrArrToStr(oriArgs, " ")+"\n");
		General.dispInfo(outStr_report,"...............baseRankLabel:"+baseRankLabel+"   ............. ............. ");
		General.dispInfo(outStr_report,"num_topLocations:"+conf.get("num_topLocations")+", sameLoc: "+conf.get("isSameLocScales"));
			
		geoRedunBinInfoPath=hdfs_address+workDir+"geoRedunBinInfo"+baseRankLabel+".InfoStr";
		
		//parameters for locListProc, -DreRankScales=10,20,50,100, -DvisScales=10,20,50,100, -DgeoExpanScales=0.001,0.01,0.1, -DisNoSameUser=true, -Dis1U1P=true,false
		int[] reRankScales=General.StrArrToIntArr(conf.get("reRankScales").split(",")); //10,20,50,100
		int[] visScales=General.StrArrToIntArr(conf.get("visScales").split(",")); //10,20,50,100
		float[] geoExpanScales=General.StrArrToFloatArr(conf.get("geoExpanScales").split(","));//0.001,0.01,0.1
		boolean[] isNoSameUsers=General.StrArrToBooleanArr(conf.get("isNoSameUser").split(","));//true,false;
		boolean[] is1U1Ps=General.StrArrToBooleanArr(conf.get("is1U1P").split(","));//true,false
		//parameters for GVM
		int[] GVM_smoothFactors=General.StrArrToIntArr(conf.get("GVM_smoothFactors").split(",")); //10,20,30,50,100
		int[] GVM_blockPortions=General.StrArrToIntArr(conf.get("GVM_blockPortions").split(",")); //2,4,10,20,40,100
		//make taskLabels
		taskLabels=new ArrayList<>();
		for (boolean isNoSameUser : isNoSameUsers) {
			taskLabels.add("_VisNN"+LocListProc.setLocListParams(0f, 100, 100, isNoSameUser, false));
			for (boolean is1U1P : is1U1Ps) {
				for (int reRankScale:reRankScales){
					for (int visScale:visScales){ // loop over parameters
						if ((reRankScale<=visScale)) {
							for(double geoExpanScale:geoExpanScales){
								String locListPara=LocListProc.setLocListParams((float) geoExpanScale, reRankScale, visScale, isNoSameUser, is1U1P);
								//*********  run GVR ***************
								taskLabels.add("_GVR"+locListPara);//
								//*********  run GVM ***************
								for (int smoothFactor:GVM_smoothFactors){
									taskLabels.add("_GVM-fInd-IDF@"+smoothFactor+locListPara);//
									taskLabels.add("_GVM-fInd-noIDF@"+smoothFactor+locListPara);//
									for (int blockPortion:GVM_blockPortions){
										taskLabels.add("_GVM-blockInd-IDF@"+smoothFactor+"@"+blockPortion+locListPara);//
										taskLabels.add("_GVM-blockInd-noIDF@"+smoothFactor+"@"+blockPortion+locListPara);//
									}
								}
							}
						}
					}
				}
			}
		}
		General.dispInfo(outStr_report,General.dispTimeDate(System.currentTimeMillis(), dateFormate)+", start processing "+taskLabels.size()+" taskLabels!  ..................");

		//*********  run taskLabels ***************
//		style1_oneJobOneScheme(redNum_style1);
		
		style2_oneJobAllScheme(reducerInter_style2);
		
		General.closePrintWriterOnExist(outStr_report);
		General_Hadoop.deleteIfExist(geoRedunBinInfoPath, hdfs);
		hdfs.close();
		return 0;
	}
	
	//******************************************************
	//********* style1_oneJobOneScheme *******************
	//******************************************************
	public void style1_oneJobOneScheme(int RedNumForProcRank) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException{
		for (String taskLabel : taskLabels) {
			oneJobScheme(taskLabel, baseRankLabel, saveRes,  conf,  hdfs, outStr_report, visRankPath, workDir,RedNumForProcRank,geoRedunBinInfoPath);
		}
	}
	
	public void oneJobScheme(String taskLabel, String baseRankLabel, boolean saveRes, Configuration conf, FileSystem hdfs, 
			PrintWriter outStr_report, String visRankPath, String Out, int RedNumForProcRank, String geoRedunBinInfoPath) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException{
		//set label
		conf.set("taskLabel",taskLabel);
		General.dispInfo(outStr_report,"\n .......task label:"+taskLabel);
		//set commons
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		String out_reranked=Out+"rank"+taskLabel+baseRankLabel; //output path for reranked ranks
		//******* 1st job: query rerank  ******
		//set input/output path
		String job1_out=out_reranked+"_tempJob1Out";
		//set distributed cache, add latlons and concept Mapfile to Distributed cache
		cacheFilePaths.clear();
		cacheFilePaths.add(hdfs_address+conf.get("latlons")+"#latlons.file"); //latlons path with symLink
		if(taskLabel.contains("_noSameUser")||taskLabel.contains("_1U1P")){
			cacheFilePaths.add(hdfs_address+conf.get("userIDs_0")+"#userIDs_0.file"); //userIDs_0
			cacheFilePaths.add(hdfs_address+conf.get("userIDs_1")+"#userIDs_1.file"); //userIDs_1
		}
		//set reducer number
		int job1RedNum=RedNumForProcRank; //reducer number
		//run
		General_Hadoop.Job(conf, new Path[]{new Path(visRankPath)}, job1_out, "process", job1RedNum, 8, 2, true,
				MapRed_GVM_old.class, null, Partitioner_random.class,null,null,Reducer_processRankDocMatches.class,
				IntWritable.class, PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr.class, IntWritable.class, fistMatch_GTruth_Docs_Locations.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		
		//******* 2nd job: save, combine querys result ******
		if (saveRes) {
			//------------- combine querys result -----------------//
			General_Hadoop.Job(conf, new Path[]{new Path(job1_out)}, out_reranked, "save", 1, 8, 2, true,
					MapRed_GVM_old.class, null, null,null,null,Reducer_InOut_1key_1value.class,
					IntWritable.class, fistMatch_GTruth_Docs_Locations.class, IntWritable.class,fistMatch_GTruth_Docs_Locations.class,
					SequenceFileInputFormat.class, MapFileOutputFormat.class, 0L, 10,
					null,null);
		}
		//******* 3nd job: analysis querys result******
		if (outStr_report!=null) {
			//run getEquBinForGeoRedundency
			if (!hdfs.exists(new Path(geoRedunBinInfoPath))) {//this is the first time to make geoRedunBinInfo
				conf.set("InfoStrPath",geoRedunBinInfoPath); // save String object to InfoStrPath
				General_Hadoop.Job(conf, new Path[]{new Path(job1_out)}, null, "getEquBin", 1, 8, 2, false,
						MapRed_GVM_old.class, null, null,null,null,Reducer_getEquBinForGeoRedundency.class,
						IntWritable.class, fistMatch_GTruth_Docs_Locations.class, IntWritable.class,fistMatch_GTruth_Docs_Locations.class,
						SequenceFileInputFormat.class, NullOutputFormat.class, 0L, 0,
						null,null);
			}
			String Info=(String) General_Hadoop.readObject_HDFS(hdfs, geoRedunBinInfoPath);
			conf.set("binsForGeoRedun",Info); //for Job3		
			//set info
			String InfoStrPath=hdfs_address+out_reranked+".InfoStr";
			conf.set("InfoStrPath",InfoStrPath); //Job3 save MAPInfo as String object to InfoStrPath
			//set distributed cache, add latlons to Distributed cache
			cacheFilePaths.clear();
			cacheFilePaths.add(hdfs_address+conf.get("latlons")+"#latlons.file"); //latlons path with symLink
			//run
			General_Hadoop.Job(conf, new Path[]{new Path(job1_out)}, null, "Analysis", 1, 8, 2, true,
					MapRed_GVM_old.class, null, null,null,null,Reducer_makeReport.class,
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
		hdfs.delete(new Path(job1_out), true);
		General.dispInfo(outStr_report,General.dispTimeDate(System.currentTimeMillis(), "yyyy.MM.dd G 'at' HH:mm:ss z")+", finished!  ");	
	}

	//******* 1st job: query rerank  ******
	public static class Reducer_processRankDocMatches extends Reducer<IntWritable,PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr,IntWritable,fistMatch_GTruth_Docs_Locations>  {

		//for user
		private UserIDs userIDs;
		//for calculating groundTruth
		private float G_ForGTSize;
		private int V_ForGTSize;
		private float[][] latlons;
		//GVM
		GVM proc_GVM;	
		//common
		private int queryNum;
		private boolean disp;
		private int dispInter;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
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
			userIDs=new UserIDs((long[]) General.readObject("userIDs_0.file"),(int[]) General.readObject("userIDs_1.file")); 
			//******* read num_topLocations  **************
			int num_topLocations=Integer.valueOf(conf.get("num_topLocations"));
			//******* read parameters used in GVM  **************
			proc_GVM=new GVM(taskLabel, userIDs, latlons, num_topLocations, null, null, G_ForGTSize, V_ForGTSize);
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			disp=false;
			queryNum=0; //total point number in one reducer
			dispInter=100;
			
	 	}
		
		@Override
		public void reduce(IntWritable QueryName, Iterable<PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr> docs_scores_matches_I, Context context) throws IOException, InterruptedException {
			//docs_scores: ranked docNames and Scores
			int queryName=QueryName.get(); 
			PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr temp=General_Hadoop.readOnlyOneElement(docs_scores_matches_I);
			if (queryNum%dispInter==0) {
				disp=true;
				System.out.println("\t start process "+queryNum+"-th query!");
			}
			fistMatch_GTruth_Docs_Locations outValue=proc_GVM.ProcessOneQuery(queryName, temp, disp);
			context.write(QueryName, outValue);
			queryNum++;
			disp=false;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one reducer finished! total query number: "+queryNum);
			
	 	}
	}

	//******* 2nd job: getEquBinForGeoRedundency******
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
			
	 	}
	}

	//******* 3rd job: analysis query result******
	public static class Reducer_makeReport extends Reducer<IntWritable,fistMatch_GTruth_Docs_Locations,IntWritable,fistMatch_GTruth_Docs_Locations>  {
		private Configuration conf;
		private FileSystem hdfs;
		GVM_Evaluator evaluator;
		private String InfoStrPath;
		private int queryNums;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			hdfs=FileSystem.get(conf);
			//******* read taskLabel**************
			String taskLabel=conf.get("taskLabel");
			System.out.println("taskLabel: "+taskLabel);
			//******* set GVM_Evaluator  **************
			evaluator=new GVM_Evaluator(conf);
			
			//**** set InfoStrPath ************//
			InfoStrPath=conf.get("InfoStrPath");

			queryNums=0;
			// ***** setup finsihed ***//
			System.out.println("only 1 reducer, combine result and analysize performance, save String obj to InfoStrPath: "+InfoStrPath);
			System.out.println("setup finsihed!");
			
	 	}
		
		@Override
		public void reduce(IntWritable Key_queryName, Iterable<fistMatch_GTruth_Docs_Locations> geoExpansionData, Context context) throws IOException, InterruptedException {
			//key: queryName, value: rank result
			
			//******** only one list in rank result! ************		
			int queryName=Key_queryName.get(); fistMatch_GTruth_Docs_Locations locationRank = General_Hadoop.readOnlyOneElement(geoExpansionData);
			//******* analysis this query's result by different evaluation radius *********************
			evaluator.addOneQueryRank(queryName, locationRank);
			queryNums++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			String outInfo=evaluator.getEvalationRes();
			//outPut
			General_Hadoop.writeObject_HDFS(hdfs, InfoStrPath, outInfo);
			
			// ***** setup finsihed ***//
			System.out.println("\n Reducer finished! total querys:"+queryNums);
			System.out.println("outInfo: \n"+ outInfo);
			
	 	}
	}
	
	//******************************************************
	//********* style2_only 1 Job for all schemes **********
	//******************************************************
	public void style2_oneJobAllScheme(int reducerInter) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException{
		int JobID=new Random().nextInt();
		//******* 0: getEquBinForGeoRedundency, use the first taskLabel to lunch oneJobOneScheme to get the conf for EquBinForGeoRedundency ******
		oneJobScheme(taskLabels.get(0), baseRankLabel, false,  conf,  hdfs, outStr_report, visRankPath, workDir, 700, geoRedunBinInfoPath);
		//schemeLabels
		String In_schemeLabels=workDir+"schemeLabels"+JobID+".seq";
		General_Hadoop.makeTextSeq_indIsKey(new Path(In_schemeLabels), taskLabels, conf);//ind in schemeLabels is the reducer ind 
		//******* 1: Paradise schemeLabels, do final ranking ******
		String out_reportTemp=workDir+JobID+"_result.report";
		conf.set("reducerInter", reducerInter+"");
		Partitioner_equalAssign partitioner_ParaFlag=new Partitioner_equalAssign(conf,false);
		//add Distributed cache
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		cacheFilePaths.add(hdfs_address+conf.get("latlons")+"#latlons.file"); //latlons path with symLink
		cacheFilePaths.add(hdfs_address+conf.get("userIDs_0")+"#userIDs_0.file"); //userIDs_0
		cacheFilePaths.add(hdfs_address+conf.get("userIDs_1")+"#userIDs_1.file"); //userIDs_1
		cacheFilePaths.add(visRankPath+"#iniVisualRank.seqFile"); //iniVisualRank.mapFile path with symLink
		General_Hadoop.Job(conf, new Path[]{new Path(In_schemeLabels)}, out_reportTemp, "proccess"+taskLabels.size(), partitioner_ParaFlag.getReducerNum(taskLabels.size()), 8, 10, true,
				MapRed_GVM_old.class, null, partitioner_ParaFlag.getPartitioner(), null, null, Reducer_paraliseScheme.class,
				IntWritable.class, Text.class, IntWritable.class, Text.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		// *********2: combine report ************
		String InfoStrPath=workDir+JobID+".ResAnaInfoStr";
		outStr_report.println(Reducer_combineReport.Job_combineReports(conf, hdfs, out_reportTemp, InfoStrPath, MapRed_GVM_old.class, "combineReport", true));
		General.dispInfo(outStr_report,General.dispTimeDate(System.currentTimeMillis(), "yyyy.MM.dd G 'at' HH:mm:ss z")+", finished!  ");	
		//clean up
		General_Hadoop.deleteIfExist(In_schemeLabels, hdfs);
		General_Hadoop.deleteIfExist(out_reportTemp, hdfs);
	}
	
	public static class Reducer_paraliseScheme extends Reducer<IntWritable,Text,IntWritable,Text>  {

		Configuration conf;
		//for user
		private UserIDs userIDs;
		//for calculating groundTruth
		private float G_ForGTSize;
		private int V_ForGTSize;
		private float[][] latlons;
		private int num_topLocations;
		private String iniRank;
		//common
		private int schemeNum;
		private boolean disp;
		private int dispInter;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			
			//******* read G_ForGTSize, V_ForGTSize**************
			G_ForGTSize=Float.valueOf(conf.get("G_ForGTSize"));
			V_ForGTSize=Integer.valueOf(conf.get("V_ForGTSize"));
			System.out.println("G_ForGTSize: "+G_ForGTSize+", V_ForGTSize:"+V_ForGTSize);
			//check file in distributted cache
			General.checkDir(new Disp(true,"",null), ".");
			latlons=(float[][]) General.readObject("latlons.file");
			userIDs=new UserIDs((long[]) General.readObject("userIDs_0.file"),(int[]) General.readObject("userIDs_1.file")); 
			iniRank="iniVisualRank.seqFile";
			//******* read num_topLocations  **************
			num_topLocations=Integer.valueOf(conf.get("num_topLocations"));
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			disp=false;
			schemeNum=0; //total point number in one reducer
			dispInter=2;
			
	 	}
		
		@Override
		public void reduce(IntWritable schemeID, Iterable<Text> schemeLabels, Context context) throws IOException, InterruptedException {
			//******* read taskLabel**************
			Text schemeLabel=General_Hadoop.readOnlyOneElement(schemeLabels);
			//******* disp **************
			long startTime=System.currentTimeMillis();
			if (schemeNum%dispInter==0) {
				disp=true;
				System.out.println("\n start process "+schemeNum+"-th scheme! schemeLabel:"+schemeLabel);
			}
			//******* prepare GVM  **************
			GVM proc_GVM=new GVM(schemeLabel.toString(), userIDs, latlons, num_topLocations, null, null, G_ForGTSize, V_ForGTSize);
			GVM_Evaluator evaluator=new GVM_Evaluator(conf);
			//******* read rank and process *********
			SequenceFile.Reader DocMatchReader= General_Hadoop.openSeqFileInNode(iniRank, conf, disp);
			IntWritable Key_queryName=new IntWritable();
			PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr Value_RankScores= new PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr();
			while (DocMatchReader.next(Key_queryName, Value_RankScores)) {
				fistMatch_GTruth_Docs_Locations locationRank=proc_GVM.ProcessOneQuery(Key_queryName.get(),Value_RankScores,false);
				evaluator.addOneQueryRank(Key_queryName.get(), locationRank);
			}
			DocMatchReader.close();
			String outInfo=evaluator.getEvalationRes();
			//outPut
			context.write(schemeID, new Text("\n"+schemeLabel+"\n"+outInfo));
			General.dispInfo_ifNeed(disp, "", " this scheme is done! "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", result: \n"+ outInfo);
			schemeNum++;
			disp=false;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one reducer finished! total scheme number: "+schemeNum);
			
	 	}
	}
}
