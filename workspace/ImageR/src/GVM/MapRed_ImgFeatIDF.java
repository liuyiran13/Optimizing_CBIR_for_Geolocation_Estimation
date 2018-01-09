package GVM;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.Obj.Statistics;
import MyAPI.imagR.IDF;
import MyCustomedHaoop.Partitioner.Partitioner_random_sameKey;
import MyCustomedHaoop.ValueClass.DID_QFeatInds;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;
import MyCustomedHaoop.ValueClass.FeatIDFs;
import MyCustomedHaoop.ValueClass.ImageRegionMatch;

public class MapRed_ImgFeatIDF extends Configured implements Tool{

	/**
	 * 
	 * @throws Exception 
	 * 
	 * 	 
	 * yarn jar MapRed_ImgFeatIDF.jar GVM.MapRed_ImgFeatIDF -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -DisDvsQ=true -DtotDocNum_forCalcuIDF=10000 MM15/ImageR/ranks/RandSelDB_MEva13_9M_20K-VW_SURF_HDs18-HMW12_Q0_0_1vs1AndHistAndAngle@0.52@0.2@1@0@0@0@0@0@0@0_verifiedMatches 1000
	 */
	
	public static void main(String[] args) throws Exception {
		runHadoop(args);
	}
	
	public static void runHadoop(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_ImgFeatIDF(), args);
		System.exit(ret);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem hdfs=FileSystem.get(conf); 
		ArrayList<String> cacheFilePaths=new ArrayList<String>();

		String imgMatchesPath=args[0];
		System.out.println("imgMatchesPath:"+imgMatchesPath);
		
		int job1_1RedNum=Integer.valueOf(args[1]); //reducer number for get feat IDF
		
		//set out path
		String out_Job1_1=imgMatchesPath+"_IDF";
				
		//******* 1st job: get imgFeatIDF based on the matches ******
		General_Hadoop.Job(conf, new Path[]{new Path(imgMatchesPath)}, out_Job1_1, "getIDF", job1_1RedNum, 8, 2, true,
				MapRed_ImgFeatIDF.class, Mapper_organiseMatches.class, Partitioner_random_sameKey.class, null,null, Reducer_getQFeatIDF.class,
				IntWritable.class, DID_QFeatInds.class, IntWritable.class, FeatIDFs.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 10,
				cacheFilePaths.toArray(new String[0]),null);

		hdfs.close();
		return 0;

	}
	
	public static class Mapper_organiseMatches extends Mapper<IntWritable,DID_Score_ImageRegionMatch_ShortArr,IntWritable,DID_QFeatInds>{
		
		private boolean isDvsQ;
		private int procSamples;
		private int dispInter;
		private long startTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//***** set isDvsQ ***//
			isDvsQ=Boolean.valueOf(conf.get("isDvsQ")); //true means when build matches, use db photos to seach through Q index, this is for small db but big Q
			System.out.println("isDvsQ: "+isDvsQ+", this is true means when build matches, use db photos to seach through Q index, this is for small db but big Q!");
			//set procSamples
			procSamples=0;
			//set dispInter
			dispInter=5000;
			startTime=System.currentTimeMillis(); //startTime
			System.out.println("mapper setup finsihed!");
	 	}
		
		@Override
		protected void map(IntWritable key, DID_Score_ImageRegionMatch_ShortArr value, Context context) throws IOException, InterruptedException {
			//key: QID
			//value: DID_Score_ImageRegionMatch_ShortArr
			procSamples++;
			if (isDvsQ) {
				for (ImageRegionMatch oneMatch : value.getMatches()) {
					LinkedList<Integer> docFeatInd=new LinkedList<>();//only one element
					docFeatInd.add(oneMatch.dst);
					context.write(new IntWritable(value.getDID()), new DID_QFeatInds(key.get(), docFeatInd));
				}
			}else {//normal: query vs D
				LinkedList<Integer> queryFeatInds=new LinkedList<>();
				for (ImageRegionMatch oneMatch : value.getMatches()) {
					queryFeatInds.add(oneMatch.src);
				}
				context.write(key, new DID_QFeatInds(value.getDID(), queryFeatInds));
			}
			//disp
			if((procSamples)%dispInter==0){ 							
				System.out.println( procSamples+" Samples finished!!  ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples+" ....."+ General.dispTime ( System.currentTimeMillis()-startTime, "min"));
			
	 	}
	}

	public static class Reducer_getQFeatIDF extends Reducer<IntWritable, DID_QFeatInds, IntWritable, FeatIDFs>  {
		
		private int totDocNum_forCalcuIDF;
		private int queryNum;
		private long startTime;
		private int dispInter_Q;
		private HashSet<Integer> processedQuery;
		private Statistics<Integer> statistic_featOccNum;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			totDocNum_forCalcuIDF=Integer.valueOf(conf.get("totDocNum_forCalcuIDF"));
			//***** check duplicated queryID ***//
			processedQuery=new HashSet<Integer>();
			statistic_featOccNum=new Statistics<Integer>(10);
			// ***** setup finsihed ***//
			queryNum=0;
			startTime=System.currentTimeMillis();
			dispInter_Q=1000;
			System.out.println("setup finsihed!\n");
			
	 	}
			
		@Override
		public void reduce(IntWritable QID, Iterable<DID_QFeatInds> values, Context context) throws IOException, InterruptedException {
			/**
			 * 1 reduce: process docs for 1 query, key: queryID, value: docID and this doc's MatchFeats for this query
			 * 
			 */
			General.Assert(processedQuery.add(QID.get()), "err! duplicated queryID:"+QID.get());

			queryNum++;
			//run
			int docNum=0; IDF idf=new IDF(true, totDocNum_forCalcuIDF);
			for (DID_QFeatInds docAllMatchFeats : values) {
				for (int qFeatInd : docAllMatchFeats.getQFeatInds()) {
					idf.updateOneIterm(qFeatInd);
				}
				docNum++;
			}
			//out put
			context.write(QID, idf.getFeatIDFs());
			//updata global state_featOccNum
			for (Entry<Integer, Integer> qFeatInd_docFreq : idf.getAllFreqs()) {
				statistic_featOccNum.addSample(qFeatInd_docFreq.getValue(), QID.get());
			}
			//done
			General.dispInfo_ifNeed(queryNum%dispInter_Q==0, "", queryNum+"queries are done! current finished queryID: "+QID+", matched docNum:"+docNum+"\n"
					+"idf statistics for this Q: "+idf.getIDFStatistic(10)+"\n"
					+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );						
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** finsihed ***//			
			System.out.println("one Reducer finished! total querys in this reducer:"+queryNum+"\n"
					+ "statistic_featOccNum: "+statistic_featOccNum.getFullStatistics("0")+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
	 	}
	
	}
	
}
