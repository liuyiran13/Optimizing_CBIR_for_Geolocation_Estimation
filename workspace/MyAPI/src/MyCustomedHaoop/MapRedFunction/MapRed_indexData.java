package MyCustomedHaoop.MapRedFunction;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import MyAPI.General.General_Hadoop;

public class MapRed_indexData{

	/**
	 *
	 *	 index all data-pair, index from 0, and count data point number
	 */

	@SuppressWarnings("rawtypes")
	public static int runHadoop(Configuration conf, Path[] input, String outPath, Class dataKeyClass, Class dataValueClass) throws Exception {		
		String objDest=outPath+"_tempForMapRed_indexData.int";
		conf.set("objDest", objDest);
		//******* 1st job: count data point number ******
		General_Hadoop.Job(conf, input, outPath, "indexData", 1, 8, 2,  true, 
				MapRed_indexData.class, null, null, null, null,Reducer_index.class,
				dataKeyClass, dataValueClass, IntWritable.class,dataValueClass,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
				null,null);
		int totNum=(Integer) General_Hadoop.readObject_HDFS(FileSystem.get(conf), objDest);
		FileSystem.get(conf).delete(new Path(objDest), true);
		return totNum;
	}
	
	//******** job1_1 **************	
	public static class Reducer_index <K,V extends Writable>extends Reducer<K,V,IntWritable,V>{

		int dataIndex;
		int reduceNum;
		String objDest;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			dataIndex=0;
			reduceNum=0;
			objDest=conf.get("objDest");
			System.out.println("objDest:"+objDest);
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void reduce(K key, Iterable<V> value, Context context) throws IOException, InterruptedException {
			//********  ************	
			for(Iterator<V> it=value.iterator();it.hasNext();){// loop over all BufferedImage_jpg				
				V one=it.next();
				context.write(new IntWritable(dataIndex), one);
				dataIndex++;
			}
			reduceNum++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("finished! reduceNum: "+reduceNum+", totNum:"+dataIndex);
			if (objDest!=null) {
				General_Hadoop.writeObject_HDFS(FileSystem.get(context.getConfiguration()), objDest, dataIndex);
				System.out.println("object:totNum is saved to "+objDest);
			}
			super.setup(context);
	 	}
	}
}
