package BuildRank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import MyAPI.General.General_Hadoop;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.IntList_FloatList;

public class MapRed_CheckRank extends Configured implements Tool{

	/** 
	 * 
	 * Job_checkQueryNum: read query rank, and check rank
	 * mapper: read and out, read query rank from MapFile(only use data), output: queryName_(docNames_scores)
	 * reducer: check queryNum
	 * 
	 * @param: 
	 * 
	 * @throws Exception 
	 * @command_example: 
	 * hadoop jar checkRank.jar BuildRank.MapRed_CheckRank -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV.jar,EJML.jar,GeoRegression.jar,libpja.jar,MyAPI.jar,JSAT_r413.jar -Dmapred.task.timeout=600000 ICMR2013/SearchResult_D10M_Q100K/part-r-00000/data ICMR2013/noOutPut
	 */
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_CheckRank(), args);
		System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs=FileSystem.get(conf);
		String[] otherArgs = args; //use this to parse args!
		//set common
		String In, out;
		
		//******* run for checkQueryNum***********
		In=otherArgs[0]; //input path
		out=otherArgs[1]; //output path
		int job1RedNum=1;
		System.out.println("run for check query number");
		System.out.println("In:"+In);
		System.out.println("out:"+out);
		//1st job: check rank 
		Job_checkQueryNum(conf, In, out+"_temp", "checkQueryNum",job1RedNum);
		
//		//*****  2nd job: check photos  ******
//		In=otherArgs[0]; //input path
//		out=otherArgs[1]; //output path
//		int job2RedNum=1;
//		double Sym1M=1000*1000;
//		int saveInterval=1000*1000; 
//		int MapFileNumPerloop=10;
//		int QFileNum_start=24; //should start from 3
//		int QFileNum_end=44;  //full end with 65
//		int start_loop=QFileNum_start; 
//		int end_loop=QFileNum_end;  
//		//set image sequence file paths parameters
//		List<Path> imageSeqPaths = new ArrayList<Path>();
//		for(int loop_i=start_loop;loop_i<=end_loop;loop_i++){//one loop
//			//set photo range for one file
//			int[] photoRang=new int[2];
//			if(loop_i==3){
//				photoRang[0]=3185259;
//			}else{
//				photoRang[0]=loop_i*saveInterval;
//			}
//			photoRang[1]=(loop_i+1)*saveInterval-1;
//			for(int fi=0;fi<MapFileNumPerloop;fi++)
//				imageSeqPaths.add(new Path(In+"_"+photoRang[0]/Sym1M+"_"+photoRang[1]/Sym1M+"_MapFile/part-r-"+General.StrleftPad(fi+"", 0, 5, "0")+"/data"));
//		}	
//		Job_checkPhotos(conf, (Path[]) imageSeqPaths.toArray(new Path[imageSeqPaths.size()]), out+"_temp", "getScores_Q_"+start_loop+"_"+end_loop,job2RedNum);
		
		fs.close();

		return 0;
	}
	
	public void Job_checkQueryNum(Configuration conf, String inPath, String outPath, String JobName, int jobRedNum) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = new Job(conf, JobName); 
		//define which jar to find all class in job config (classes below) 
		job.setJarByClass(MapRed_CheckRank.class);
		//set mapper, reducer, partitioner
		job.setMapperClass(Mapper_readRank.class);  //Mapper_randQueryFroFeat, Mapper_selectQueryFroFeat, Mapper_QueryFroImage
		job.setReducerClass(Reducer_checkQueryNum.class);
		job.setNumReduceTasks(jobRedNum);
		//set mapper out-put Key_Value
		job.setMapOutputKeyClass(IntWritable.class); //if not set, MapOutputKeyClass will be OutputKeyClass
		job.setMapOutputValueClass(IntList_FloatList.class);
//		//set job out-put Key_Value
//		job.setOutputKeyClass(IntWritable.class);
//		job.setOutputValueClass(TrueRank_GTSize_Docs_GVSizes_docScores.class);
		//set job in/out FileClass
		job.setInputFormatClass(SequenceFileInputFormat.class); //SequenceFileInputFormat
		job.setOutputFormatClass(NullOutputFormat.class);
		//set job in/out file Path
		FileInputFormat.setInputPaths(job, inPath);//commaSeparatedPaths
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		if (!job.waitForCompletion(true)) {
	        throw new InterruptedException("Job Failed! job: "+job.getJobName());
		}
	}
	
	public void Job_checkPhotos(Configuration conf, Path[] inPath, String outPath, String JobName, int jobRedNum) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = new Job(conf, JobName);
		//define which jar to find all class in job config (classes below) 
		job.setJarByClass(MapRed_CheckRank.class);
		//set mapper, reducer, partitioner
		job.setMapperClass(Mapper_checkPhotoIndex.class);
		job.setReducerClass(Reducer_checkPhotoIndex.class);
		job.setNumReduceTasks(jobRedNum);
		//set mapper out-put Key_Value
		job.setMapOutputKeyClass(IntWritable.class); //if not set, MapOutputKeyClass will be OutputKeyClass
		job.setMapOutputValueClass(IntWritable.class);
//		//set job out-put Key_Value
//		job.setOutputKeyClass(IntWritable.class);
//		job.setOutputValueClass(IntWritable.class);
		//set job in/out FileClass
		job.setInputFormatClass(SequenceFileInputFormat.class); //SequenceFileInputFormat
		job.setOutputFormatClass(NullOutputFormat.class);
		//set job in/out file Path
		FileInputFormat.setInputPaths(job, inPath);//commaSeparatedPaths
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		//set job status 
		conf.setBoolean("mapred.sucess", true);
		//wait for completion
		if (!job.waitForCompletion(true)) {
	        throw new InterruptedException("Job Failed! job: "+job.getJobName());
		}
	}
	
	public static class Mapper_readRank extends Mapper<IntWritable,IntList_FloatList,IntWritable,IntList_FloatList>{

		private boolean disp;
	
		protected void setup(Context context) throws IOException, InterruptedException {
			// ***** setup finished ***//
			System.out.println("setup finsihed!");
			disp=true; 
			super.setup(context);
	 	}
		
		protected void map(IntWritable key, IntList_FloatList value, Context context) throws IOException, InterruptedException {
			//key: photoName, value: (ranked) docNames_scores	
			if (disp==true){ //debug disp info
				System.out.println("Mapper: read and out");
				System.out.println("mapIn_Key, queryName: "+key.get());
				System.out.println("mapIn_Value, ranked doc_scores, length: "+value.getIntegers().size());
				System.out.println("mapIn_Value, ranked doc_scores, sample, 1st doc&scores: "+value.getIntegers().get(0)+"_"+value.getFloats().get(0));
				System.out.println("mapIn_Value, ranked doc_scores, sample, 2nd doc&scores: "+value.getIntegers().get(1)+"_"+value.getFloats().get(1));
			}
			//** output, set key, value **//
			context.write(key, value);
			//debug disp info
			if (disp==true){ 
				disp=false;
				System.out.println("disp:"+disp);
			}
		}
	}
	
	public static class Mapper_checkPhotoIndex extends Mapper<IntWritable,BufferedImage_jpg,IntWritable,IntWritable>{
		private boolean disp;
		private Configuration conf;
		private HashMap<Integer,Integer> selectedPhotos;
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			disp=true; 
			conf = context.getConfiguration();
			FileSystem fs=FileSystem.get(conf);
			//***** read selected photos ***//
			Path selectPhotosPath = new Path("hdfs://p-head03.alley.sara.nl/user/yliu/"+conf.get("mapred.SelQuerys"));
			try {
				selectedPhotos= (HashMap<Integer, Integer>) General_Hadoop.readObject_HDFS(fs, selectPhotosPath.toString());
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			System.out.println("total selected querys:"+selectedPhotos.size());
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		@Override
		protected void map(IntWritable key, BufferedImage_jpg value, Context context) throws IOException, InterruptedException {
			int photoName=key.get();// photoName
			if(selectedPhotos.containsKey(photoName)){
				//get transfered photoName
				int transferdIndexInS=selectedPhotos.get(photoName);//transfer photo index in original dataset to index in selected photos
				IntWritable photoName_L=new IntWritable(photoName); IntWritable photoName_S=new IntWritable(transferdIndexInS);
				context.write(photoName_S, photoName_L);
				if(disp==true){
					System.out.println("photoName_S: "+photoName_S.get());
					System.out.println("photoName_L: "+photoName_L.get());
					disp=false;
				}
			}
		}
	}
	
	public static class Reducer_checkQueryNum extends Reducer<IntWritable,IntList_FloatList,IntWritable,IntWritable>  {

		private int queryNum;
		private int queryNum_notFirst;
		private int queryNum_mutiRankList;
		private int dispInter;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			queryNum=0; //total point number in one reducer
			queryNum_notFirst=0; ///querys that not ranked first!
			queryNum_mutiRankList=0;
			dispInter=10000;
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable QueryName, Iterable<IntList_FloatList> docs_scores_I, Context context) throws IOException, InterruptedException {
			//docs_scores: ranked docNames and Scores

			int queryName=QueryName.get(); IntList_FloatList docs_scores=new IntList_FloatList();
			int rankListNum=0; ArrayList<Integer> rankListLength=new ArrayList<Integer>();
			for(Iterator<IntList_FloatList> it=docs_scores_I.iterator();it.hasNext();){//only one element
				docs_scores=it.next();
				rankListNum++;
				rankListLength.add(docs_scores.getIntegers().size());
				if(queryName==2576565 || queryNum%dispInter==0){
					System.out.println("current query:"+queryName+", docs:"+docs_scores.getIntegers());
					System.out.println("current query:"+queryName+", scores:"+docs_scores.getFloats());
					System.out.println("current query:"+queryName+", docs length:"+docs_scores.getIntegers().size());
				}
			}
			
			//check query itself's rank
			if(docs_scores.getIntegers().get(0)==queryName){
				
			}else{//for some query, itself is not ranked first!!
				int queryRank=docs_scores.getIntegers().indexOf(queryName);
				if(queryRank==-1){//do not handle bug-query
					System.err.println("queryName:"+queryName+", queryRank:"+queryRank+", 1st doc:"+docs_scores.getIntegers().get(0)+", rankList size:"+docs_scores.getIntegers().size());
					return;
				}
				queryNum_notFirst++;
			}		
			queryNum++;
//			if(queryNum%dispInter==0){
//				System.out.println("current query:"+queryName+", rankListNum:"+rankListNum+", rankListLength:"+rankListLength);
//				System.out.println("queryNum:"+queryNum+", queryNum_notFirst:"+queryNum_notFirst+", queryNum_mutiRankList:"+queryNum_mutiRankList);
//			}
			if(rankListNum!=1)
				queryNum_mutiRankList++;
			if(queryName==2576565){
				System.out.println("current query:"+queryName+", rankListNum:"+rankListNum+", rankListLength:"+rankListLength);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one reducer finished! total query number: "+queryNum);
			System.out.println("within these querys, querys not ranked first, queryNum_notFirst: "+queryNum_notFirst+", queryNum_mutiRankList:"+queryNum_mutiRankList);
			super.setup(context);
	 	}
	}

	public static class Reducer_checkPhotoIndex extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>  {

		private int queryNum;
		private int queryNum_mutiIndexL;
//		private int dispInter;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			queryNum=0; //total point number in one reducer
			queryNum_mutiIndexL=0;
//			dispInter=1000;
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable QueryName_S, Iterable<IntWritable> QueryName_L, Context context) throws IOException, InterruptedException {
			//docs_scores: ranked docNames and Scores

			int queryName_S=QueryName_S.get(); 
			ArrayList<Integer> queryName_L=new ArrayList<Integer>();
			for(Iterator<IntWritable> it=QueryName_L.iterator();it.hasNext();){//only one element
				IntWritable one_queryName_L=it.next();
				queryName_L.add(one_queryName_L.get());
			}
			
			if(queryName_L.size()!=1)
				queryNum_mutiIndexL++;
			
			if(queryName_S==2576565){
				System.out.println("current queryName_S:"+queryName_S+", queryName_L:"+queryName_L);
			}
			
			queryNum++;
//			if(queryNum%dispInter==0){
//				System.out.println("current queryName_S:"+queryName_S+", queryName_L:"+queryName_L);
//				System.out.println("queryNum:"+queryNum+", queryNum_notFirst:"+queryNum_notFirst+", queryNum_mutiRankList:"+queryNum_mutiRankList);
//			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one reducer finished! total query number: "+queryNum);
			System.out.println("within these querys, queryNum_mutiIndexL:"+queryNum_mutiIndexL);
			super.setup(context);
	 	}
	}
	
}
