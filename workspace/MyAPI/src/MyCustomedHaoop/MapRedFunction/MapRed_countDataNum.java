package MyCustomedHaoop.MapRedFunction;


import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;

public class MapRed_countDataNum extends Configured implements Tool{

	/**
	 *	 count data point number
	 * @command_example: 
	 * hadoop jar MapRed_countDataNum.jar MyCustomedHaoop.MapRedFunction.MapRed_countDataNum  Webscope100M/ME14_Crawl/Photos Webscope100M/ME14_Crawl/	 
	 */

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_countDataNum(), args);
		System.exit(ret);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		String input=args[0];//input paths to process, seprated with ","
		String tempWorkPlace=args[1];
		int dataNum=runHadoop(getConf(), General_Hadoop.strArr_to_PathArr(input.split(",")), tempWorkPlace);
		System.out.println("dataNum: "+dataNum);
		return 0;
	}
	
	public static int runHadoop(Configuration conf, Path[] input, String workPath) throws Exception {		
		FileSystem hdfs=FileSystem.get(conf); 
		String objDest=workPath+"tempForMapRed_countDataNum.int";
		conf.set("objDest", objDest);
		//******* 1st job: count data point number ******
		General_Hadoop.Job(conf, input, null, "SampleNum", 1, 4, 2, true, 
				MapRed_countDataNum.class, Mapper_countNum.class, null, null, null,Reducer_combineNum.class,
				IntWritable.class, IntWritable.class, IntWritable.class,IntWritable.class,
				SequenceFileInputFormat.class, NullOutputFormat.class, 10*1024*1024*1024L, 0,
				null,null);
		int totNum=(Integer) General_Hadoop.readObject_HDFS(hdfs, objDest);
		hdfs.delete(new Path(objDest), true);
		return totNum;
	}
	
	//******** job1_1 **************	
	public static class Mapper_countNum <K,V extends Writable> extends Mapper<K,V,IntWritable,IntWritable>{

		private boolean disp;
		private int procSamples;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			disp=true; 
			procSamples=0;
			// ***** setup finished ***//
			System.out.println("Mapper_countNum setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void map(K key, V value, Context context) throws IOException, InterruptedException {
			//key: photoName, value: (ranked) docNames_scores	
			if (disp==true){ //debug disp info
				System.out.println("Mapper: read and out");
				System.out.println("mapIn_Key: "+key.toString());
				System.out.println("mapIn_Value: "+value.toString());
				disp=false;
			}
			procSamples++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples);
			//** output, set key, value **//
			context.write(new IntWritable(0), new IntWritable(procSamples));
			super.setup(context);
	 	}
	}
	
	public static class Reducer_combineNum extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{

		int reduceNum;
		String objDest;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			reduceNum=0;
			objDest=conf.get("objDest");
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		protected void reduce(IntWritable key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
			//key: photoName
			//value: file content
			//******** only one in value! ************	
			int loopNum=0; int totNum=0;
			for(Iterator<IntWritable> it=value.iterator();it.hasNext();){// loop over all BufferedImage_jpg				
				IntWritable one=it.next();
				totNum+=one.get();
				loopNum++;
			}
			System.out.println("finished! loopNum: "+loopNum+", totNum:"+totNum);
			General_Hadoop.writeObject_HDFS(FileSystem.get(context.getConfiguration()), objDest, totNum);
			System.out.println("object:totNum is saved to "+objDest);
			reduceNum++;
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			General.Assert(reduceNum==1, "err in Reducer_combineNum: reduceNum should ==1, but "+reduceNum);
			System.out.println("one reducer finished! reduceNum: "+reduceNum+", should ==1");
			super.setup(context);
	 	}
	}

}
