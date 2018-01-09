package RankStudy_MapRed;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.General.General_geoRank;
import MyCustomedHaoop.Partitioner.Partitioner_random;
import MyCustomedHaoop.ValueClass.IntArr;
import MyCustomedHaoop.ValueClass.IntArrArr;
import MyCustomedHaoop.ValueClass.IntList_FloatList;

public class MapRed_studyRank extends Configured implements Tool{

	/** study rank
	 * 
	 * job1: read query rank, and analysis photo frequency appeared in top rank
	 * mapper: read and out, read query rank from MapFile(only use data), output: queryName_(docNames_scores)
	 * reducer: for one query, analysis rank
	 * 
	 * @param (Mapper_readRank):  "mapred.latlons" "mapred.appearInTopRank" "mapred.isSameLocScales"
	 * 
	 * job2: combine results from job1
	 * mapper: read and output
	 * reducer: combine and make report
	 * 
	 * @param:  "mapred.reportPath" "mapred.showTopFreqPho"
	 * 
	 * @throws Exception 
	 * @command_example: 
	 * 3M:	hadoop jar MapRed_GeoVisualRanking_Vis.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,MyAPI.jar,JSAT_r413.jar,lire136.jar -Dmapred.task.timeout=6000000 -Dmapred.latlons=ICMR2013/3M_latlon.float2 -Dmapred.userIDs_0=ICMR2013/3M_userIDs_0.long -Dmapred.userIDs_1=ICMR2013/3M_userIDs_1.int -Dmapred.num_topDocs=200 -Dmapred.G_ForGTSize=0.01 -Dmapred.V_ForGTSize=10000 -Dmapred.reportPath=ICMR2013/GVR/ -Dmapred.isOneLocScales=0.01,0.1  -Dmapred.isSameLocScales=0.01,0.1  -Dmapred.num_topLocations=20 -Dmapred.binsForGTSize=0,1,5,10,20,40,100 -Dmapred.accumLevel=1,2,3,5,10,20 -Dmapred.saveRes=false -Dmapred.makeReport=true _D3M_Q100K_HD12_Vis ImageR/SearchResult_D3M_Q100K_ICMR13_HD12_topRank10K/part-r-00000/data ICMR2013/GVR/
	 * MEva13:	hadoop jar MapRed_studyRank.jar RankStudy_MapRed.MapRed_studyRank -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,MyAPI.jar,JSAT_r413.jar,lire136.jar -Dmapred.task.timeout=6000000 -Dmapred.job.priority=HIGH -Dmapred.latlons=MediaEval13/MEval13_latlons.floatArr -Dmapred.appearInTopRank=100 -Dmapred.showTopFreqPho=10 -Dmapred.reportPath=MediaEval13/studyRank/ -Dmapred.isSameLocScales=0.1  -Dmapred.makeReport=true -Dmapred.job.priority=HIGH _AnalysisPhotIDFreq_D9M_Q250K_SURFHD12 MediaEval13/ranks/SURF_D9M_Q250K_HD12_topRank10K/part-r-00000/data 100
	 */
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_studyRank(), args);
		System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem hdfs=FileSystem.get(conf);
		String homePath="hdfs://p-head03.alley.sara.nl/user/yliu/";
		String[] otherArgs = args; //use this to parse args!
		String dateFormate="yyyy.MM.dd G 'at' HH:mm:ss z";
		//set common
		Boolean makeReport=Boolean.valueOf(conf.get("mapred.makeReport"));
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		
		String jobLabel=otherArgs[0]+"_appearInTop"+conf.get("mapred.appearInTopRank");//_AnalysisPhotIDFreq_D9M_Q250K_SURFHD12
		String jobWorkDir=conf.get("mapred.reportPath");
		conf.set("mapred.jobLabel", jobLabel);
		
		PrintWriter outStr_report=null;
		if (makeReport) {
			outStr_report=new PrintWriter(new OutputStreamWriter(hdfs.create(new Path(homePath+conf.get("mapred.reportPath")+"Report"+jobLabel),false), "UTF-8"),true); 
		}
		
		General.dispInfo(outStr_report,"............... jobLabel:"+jobLabel+"   ............. ............. ");
		General.dispInfo(outStr_report,"caculate photo frequence appearInTopRank:"+conf.get("mapred.appearInTopRank")+", isSameLocScales: "+conf.get("mapred.isSameLocScales"));
		General.dispInfo(outStr_report,General.dispTimeDate(System.currentTimeMillis(), dateFormate)+", start processing!  ..................");
		
		//******* 1st job: query rank study  ******
		//set input/output path
		String job1_in = otherArgs[1]; //rank path
		String job1_out=jobWorkDir+"tempJob1Out"+jobLabel;
		//set distributed cache, add latlons and concept Mapfile to Distributed cache
		cacheFilePaths.clear();
		cacheFilePaths.add(homePath+conf.get("mapred.latlons")+"#latlons.file"); //latlons path with symLink
		//set reducer number
		int job1RedNum=Integer.valueOf(otherArgs[2]); //reducer number
		//run
		General_Hadoop.Job(conf, new Path[]{new Path(job1_in)}, job1_out, "phoFreq", job1RedNum, 8, 2, true,
				MapRed_studyRank.class, null, Partitioner_random.class,null,null,Reducer_processRank.class,
				IntWritable.class, IntList_FloatList.class, IntWritable.class, IntArr.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		
		//******* 2nd job: combine result from job1, get topFreq PhotoIDs  ******
		String topPhoIDsObjPath=homePath+jobWorkDir+"topPhoIDs"+jobLabel;
		String InfoStrPath=homePath+jobWorkDir+"InfoStr"+jobLabel;
		String job2_out=jobWorkDir+"tempJob2Out"+jobLabel;
		conf.set("mapred.topPhoIDsObjPath",topPhoIDsObjPath); //Job2 save topPhoIDs as object to topPhoIDsObjPath
		conf.set("mapred.InfoStrPath",InfoStrPath); //Job2 save Info as String object to InfoStrPath
		//run
		General_Hadoop.Job(conf, new Path[]{new Path(job1_out)}, job2_out, "combinePhoFreq", 1, 8, 2, true,
				MapRed_studyRank.class, null, null,null,null,Reducer_makePhoIDFreq.class,
				IntWritable.class, IntArr.class, IntWritable.class, IntArrArr.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0L, 0,
				null,null);

		String Info=(String) General_Hadoop.readObject_HDFS(hdfs, InfoStrPath);
		General.dispInfo(outStr_report,Info);
		
		//******* 3rd job: combine result from job1, choose topPhoIDs from job2, output photoID_[queryName_rank]  ******
		String job3_out=jobWorkDir+"mapF"+jobLabel;
		//set distributed cache, add latlons and concept Mapfile to Distributed cache
		cacheFilePaths.clear();
		cacheFilePaths.add(topPhoIDsObjPath+"#topPhoIDs.file"); //topPhoIDs path with symLink
		//run
		General_Hadoop.Job(conf, new Path[]{new Path(job2_out)}, job3_out, "getMapF", 1, 8, 2, true,
				MapRed_studyRank.class, null, null,null,null,Reducer_getTopPhoIDFreq.class,
				IntWritable.class, IntArrArr.class, IntWritable.class, IntArrArr.class,
				SequenceFileInputFormat.class, MapFileOutputFormat.class, 0L, 10,
				cacheFilePaths.toArray(new String[0]),null);
				
		//********* clean-up ***********//
		hdfs.delete(new Path(topPhoIDsObjPath), true);
		hdfs.delete(new Path(InfoStrPath), true);
		hdfs.delete(new Path(job1_out), true);
		hdfs.delete(new Path(job2_out), true);
		General.dispInfo(outStr_report,General.dispTimeDate(System.currentTimeMillis(), "yyyy.MM.dd G 'at' HH:mm:ss z")+", finished!  ");	
		if (makeReport) {
			outStr_report.flush();	
			outStr_report.close();
		}
		hdfs.close();
		return 0;
	}

	//******* 1st job: study query rank  ******
	public static class Reducer_processRank extends Reducer<IntWritable,IntList_FloatList,IntWritable,IntArr>  {

		private String jobLabel;
		private int queryNum;
		private float[][] latlons;
		private float isSameLocScales;
		private int appearInTopRank;
		private HashMap<Integer, Integer> photoFreq;
		private boolean disp;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//******* read jobLabel**************
			jobLabel=conf.get("mapred.jobLabel"); //_GlobFilter _Vis
			System.out.println("jobLabel: "+jobLabel);
			
			latlons=(float[][]) General.readObject("latlons.file");
			//******* read isSameLocScales, tappearInTopRank **************
			isSameLocScales=Float.valueOf(conf.get("mapred.isSameLocScales"));
			appearInTopRank=Integer.valueOf(conf.get("mapred.appearInTopRank"));
			//******* initialize photoFreq **************
			photoFreq=new HashMap<Integer, Integer>();
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
			General.Assert(docs_scores.getIntegers().size()== docs_scores.getFloats().size(), 
					"err in Reducer_processRank! docs and scores are not equal length! docs:"+docs_scores.getIntegers().size()+", scores:"+docs_scores.getFloats().size());
			
			//delete query itself
			General_geoRank.removeQueryItself_forTopDocsScores(docs_scores.getIntegers(), docs_scores.getFloats(), queryName);
			
			int totRank_length=docs_scores.getIntegers().size();
			for (int i = 0; i < Math.min(totRank_length, appearInTopRank); i++) {
				int visNeig=docs_scores.getIntegers().get(i);
				if(!General_geoRank.isOneLocation(latlons[0][queryName],latlons[1][queryName],latlons[0][visNeig],latlons[1][visNeig],isSameLocScales)){//not true match
					context.write(new IntWritable(visNeig), new IntArr(new int[]{queryName,i}));
				}
			}
			
			if(disp==true){
				System.out.println("1st QueryName:"+QueryName.get());
				System.out.println("current phoID_Freq: "+photoFreq);
				disp=false;
			}		
			
			queryNum++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// *******  output  ***//
			System.out.println("one reducer finished! total query number: "+queryNum);
			int[][] phoID_Freq=new int[photoFreq.size()][2];
			int ind=0;
			for (Entry<Integer, Integer> keyValue: photoFreq.entrySet()) {
				phoID_Freq[ind][0]=keyValue.getKey();
				phoID_Freq[ind][1]=keyValue.getValue();
				ind++;
			}
			System.out.println("unique photoID in phoID_Freq: "+phoID_Freq.length);
//			context.write(new IntWritable(0), new IntArrArr(phoID_Freq));
			super.setup(context);
	 	}
	}
	
	//******* 2nd job: save, combine result, and make report ******
	public static class Reducer_makePhoIDFreq extends Reducer<IntWritable,IntArr,IntWritable,IntArrArr>  {
		private Configuration conf;
		private FileSystem hdfs;
		private String topPhoIDsObjPath;
		private String InfoStrPath;
		private int showTopFreqPho;
		private int appearInTopRank;
		private HashMap<Integer, Integer> photoIDFreq;
		private int reduce_Nums;
		private boolean disp;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			hdfs=FileSystem.get(conf);

			//******* read topPhoIDsObjPath, InfoStrPath**************
			topPhoIDsObjPath=conf.get("mapred.topPhoIDsObjPath"); //save topPhoIDs as object saved on topPhoIDsObjPath
			InfoStrPath=conf.get("mapred.InfoStrPath"); //save info as string object saved on InfoStrPath
			//******* read showTopFreqPho**************
			showTopFreqPho=Integer.valueOf(conf.get("mapred.showTopFreqPho"));
			appearInTopRank=Integer.valueOf(conf.get("mapred.appearInTopRank"));
			
			photoIDFreq=new HashMap<Integer, Integer>();
			// ***** setup finsihed ***//
			disp=true;
			reduce_Nums=0;
			System.out.println("only 1 reducer, multiple reduce, each is for one photoID, save top PhotoIDs as obj, and save info as string object saved on InfoStrPath: "+InfoStrPath);
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable key, Iterable<IntArr> value, Context context) throws IOException, InterruptedException {
			//key: photoID, value: queryName_thisPhotoRankInThisQuery needed
			
			int photoID=key.get();
			//******** make freq for this photoID ************		
			int freq=0;  ArrayList<int[]> queryName_Rank=new ArrayList<int[]>();
			for(Iterator<IntArr> it=value.iterator();it.hasNext();){// loop over all HashMaps		
				queryName_Rank.add(it.next().getIntArr());
				freq++;
			}
			if (disp) {
				System.out.println("photoID:"+photoID+", tot freq:"+freq);
				disp=false;
			}
			context.write(key, new IntArrArr(General.ArrListToIntArrArr(queryName_Rank)));
			General.Assert(!photoIDFreq.containsKey(photoID), "err in Reducer_makePhoIDFreq, photoID:"+photoID+" is alreay exist in this hashMap, photoIDFreq!");
			photoIDFreq.put(photoID, freq);

			reduce_Nums++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("one reducer finished! total photoID number: "+reduce_Nums);
			//rank photoID based on Freqs
			ArrayList<Integer> topPhos=new ArrayList<Integer>(showTopFreqPho);
			ArrayList<Float> topFreqs=new ArrayList<Float>(showTopFreqPho);
			General_IR.rank_get_TopDocScores_treeSet(photoIDFreq, showTopFreqPho, topPhos, topFreqs, "DES");
			//outPut top photoID
			General_Hadoop.writeObject_HDFS(hdfs, topPhoIDsObjPath, topPhos);
			//outPut info
			StringBuffer outInfo=new StringBuffer();
			outInfo.append("--------- within top-"+appearInTopRank+" ranked photos of each query, top-"+showTopFreqPho+" frequenced photos are: [Freq_PhotoID]\n");
			for (int i = 0; i < topPhos.size(); i++) {
				outInfo.append(topFreqs.get(i).intValue()+"_"+topPhos.get(i)+"\n");
			}
			System.out.println("outInfo: \n"+ outInfo.toString());
			General_Hadoop.writeObject_HDFS(hdfs, InfoStrPath, outInfo.toString());			
			super.setup(context);
	 	}
	}
	
	//******* 3rd job: save, combine result ******
	public static class Reducer_getTopPhoIDFreq extends Reducer<IntWritable,IntArrArr,IntWritable,IntArrArr>  {
		private ArrayList<Integer> topPhosList;
		private HashSet<Integer> topPhosSet;
		private int reduce_Nums;
		private boolean disp;
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			topPhosList=(ArrayList<Integer>) General.readObject("topPhoIDs.file");
			topPhosSet=new HashSet<Integer>(topPhosList);
			System.out.println("topPhosSet size: "+topPhosSet.size());
			// ***** setup finsihed ***//
			disp=true;
			reduce_Nums=0;
			System.out.println("only 1 reducer, multiple reduce, each is for one photoID, save top PhotoID_[queryName_Rank] into MapFile ");
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable key, Iterable<IntArrArr> value, Context context) throws IOException, InterruptedException {
			//key: photoID, value: int[][], [queryName_thisPhotoRankInThisQuery] needed
			
			int photoID=key.get();
			//******** make freq for this photoID ************		
			IntArrArr queryName_rank=new IntArrArr(); int loopNum=0;
			for(Iterator<IntArrArr> it=value.iterator();it.hasNext();){// loop over all HashMaps		
				queryName_rank=it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_processRank! one photoName, one ranklist, loopNum should == 1, here loopNum="+loopNum);
			if (topPhosList.contains(photoID)) {
				context.write(key, queryName_rank);
			}
			if (disp) {
				System.out.println("photoID:"+photoID+", tot freq:"+queryName_rank.getArrArr().length);
				disp=false;
			}

			reduce_Nums++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("one reducer finished! total photoID number: "+reduce_Nums);
			super.setup(context);
	 	}
	}
	
}
