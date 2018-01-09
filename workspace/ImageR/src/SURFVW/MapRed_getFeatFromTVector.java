package SURFVW;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyCustomedHaoop.ValueClass.IntArr_byteArrArrArr_Short;
import MyCustomedHaoop.ValueClass.Int_ByteArrList;

public class MapRed_getFeatFromTVector extends Configured implements Tool{

	/**
	 * 7157530, 7348719
	 * MEva13:	hadoop jar MapRed_getFeatFromTVector.jar SURFVW.MapRed_getFeatFromTVector -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 7348719 MediaEval13/TVector_MEva13_9M_MapFile/ 1000 MediaEval13/oneQuery/photoFeat_onDocDebug
	 * 
	 */
	
	public static void main(String[] args) throws Exception {		
		runHadoop(args);
	}
	
	public static void runHadoop(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_getFeatFromTVector(), args);
		System.exit(ret);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs=FileSystem.get(conf); 
		String homePath="hdfs://p-head03.alley.sara.nl/user/yliu/";
		String[] otherArgs = args; //use this to parse args!
		
		int doc=Integer.valueOf(otherArgs[0]);
		
		String TVectorPath=homePath+otherArgs[1]; int fileNum=Integer.valueOf(otherArgs[2]);
		String featSavePath=homePath+otherArgs[3]+"_"+doc;
		
//		//run local
//		long startTime=System.currentTimeMillis();
//		System.out.println("TVectorPath: "+TVectorPath);
//		HashMap<Integer, ArrayList<byte[]>> VW_Sigs_doc = General_Hadoop.extractFeatFromTVector(doc, TVectorPath, fileNum, 10, System.currentTimeMillis());
//		System.out.println("\t extract VW_Sigs from TVector finished! vw number: "+VW_Sigs_doc.size()+", "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//		General_Hadoop.writeObject_HDFS(fs, featSavePath, VW_Sigs_doc);

		//run MapRed
		conf.set("mapred.targetDoc", doc+""); 
		conf.set("mapred.featSavePath", featSavePath); 
		ArrayList<Path> TVectorPaths = new ArrayList<Path>();
		for(int file_i=0; file_i<fileNum; file_i++){
			TVectorPaths.add(new Path(TVectorPath+"part-r-"+General.StrleftPad(file_i+"", 0, 5, "0")+"/data"));
		}
		System.out.println("TVectorPath: "+TVectorPath+", fileNum:"+TVectorPaths.size());
		General_Hadoop.Job(conf, TVectorPaths.toArray(new Path[0]), null, "getFeatFromTVec", 1, 8, 2, false,
				MapRed_getFeatFromTVector.class, Mapper_readFeatFromTVector.class, null,null,null,Reducer_saveFeatToHashMap.class,
				IntWritable.class, Int_ByteArrList.class, IntWritable.class,IntWritable.class,
				SequenceFileInputFormat.class, NullOutputFormat.class, 10*1024*1024*1024L, 0,
				null,null);
		
		fs.close();
		return 0;
	}
	
	public static class Mapper_readFeatFromTVector extends Mapper<IntWritable,IntArr_byteArrArrArr_Short,IntWritable,Int_ByteArrList>{

		private int targetDoc;
		private boolean disp;
		private int procSamples;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			targetDoc=Integer.valueOf(conf.get("mapred.targetDoc"));
			System.out.println("targetDoc: "+targetDoc);
			disp=true; 
			procSamples=0;
			// ***** setup finished ***//
			System.out.println("Mapper_readFeatFromTVector setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void map(IntWritable key, IntArr_byteArrArrArr_Short value, Context context) throws IOException, InterruptedException {
			//key: vw, value: photoName_sigs
			int targetDocInd=-1;
			int[] docs=value.getIntegers();
			for (int doc_i=0; doc_i<docs.length; doc_i++) {
				if (docs[doc_i]==targetDoc) {
					targetDocInd=doc_i;
				}
			}
			if (targetDocInd>-1) {//targetDoc exist this vw
				context.write(new IntWritable(targetDoc), new Int_ByteArrList(key.get(), General.ByteArrArrToListByteArr(value.getbyteArrArrArr()[targetDocInd]))) ;
			}
			
			if (disp==true){ //debug disp info
				System.out.println("Mapper: Mapper_readFeatFromTVector");
				System.out.println("key(vw): "+key+", photoNum:"+value.getIntegers().length);
				disp=false;
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
	
	public static class Reducer_saveFeatToHashMap extends Reducer<IntWritable,Int_ByteArrList,IntWritable, IntWritable>  {
		private String featSavePath;
		private int sampleNums;
		private FileSystem hdfs;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			hdfs=FileSystem.get(conf); 
			
			featSavePath=conf.get("mapred.featSavePath"); 
			sampleNums=0;
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable sampleName, Iterable<Int_ByteArrList> values, Context context) throws IOException, InterruptedException {
			//key: sampleName, value: vw_sigs

			//******** loop over all values! ************		
			HashMap<Integer,ArrayList<byte[]>> VW_Sigs_doc=new HashMap<Integer, ArrayList<byte[]>>();
			int loopNum=0;  Int_ByteArrList oneSample=null;
			for(Iterator<Int_ByteArrList> it=values.iterator();it.hasNext();){// loop over vw_sigs		
				oneSample=it.next();
				VW_Sigs_doc.put(oneSample.getInt(), oneSample.getbyteArrs());
				loopNum++;
			}
			System.out.println("finsihed! loopNum:"+loopNum);
			
			General_Hadoop.writeObject_HDFS(hdfs, featSavePath, VW_Sigs_doc);
			sampleNums++;
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("read and out finished! total sampleNums:"+sampleNums);
			hdfs.close();
			super.setup(context);
	 	}
	}
}
