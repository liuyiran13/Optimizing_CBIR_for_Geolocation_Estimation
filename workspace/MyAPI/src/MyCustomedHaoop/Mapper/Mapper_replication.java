package MyCustomedHaoop.Mapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import MyCustomedHaoop.KeyClass.Key_RankFlagID_QID;

public class Mapper_replication {
	
	public static void setReplicationNum(Configuration conf, int replicationNum){
		conf.set("replicationNum_samp", replicationNum+"");
	}
	
	public static int getReplicationNum(Configuration conf){
		return Integer.valueOf(conf.get("replicationNum_samp"));
	}

	public static class MapperReplication_RankFlagID_QID_valueV <V extends Writable> extends Mapper<IntWritable,V,Key_RankFlagID_QID,V>{
		//do nothing, just read, out
		private boolean disp;
		private int replicationNum_samp;
		private int procSamples;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			replicationNum_samp=getReplicationNum(context.getConfiguration());
			disp=true; 
			procSamples=0;
			// ***** setup finished ***//
			System.out.println("Mapper_readRank setup finsihed! replicationNum_samp:"+replicationNum_samp);
			super.setup(context);
	 	}
		
		@Override
		protected void map(IntWritable key, V value, Context context) throws IOException, InterruptedException {
			//key: photoName, value: (ranked) docNames_scores	
			if (disp==true){ //debug disp info
				System.out.println("Mapper: read and replicate it to Key_QID_reRankFlagGroupID with "+replicationNum_samp+" times.");
				System.out.println("current key: "+key+", value: "+value);
				disp=false;
			}
			for (int i = 0; i < replicationNum_samp; i++) {
				context.write(new Key_RankFlagID_QID(i,key.get()), value);
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
	
	public static class MapperReplication_RankFlagID_QID_keyV <V extends Writable> extends Mapper<IntWritable,V,Key_RankFlagID_QID,Key_RankFlagID_QID>{
		//do nothing, just read, out
		private boolean disp;
		private int replicationNum_samp;
		private int procSamples;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			replicationNum_samp=getReplicationNum(context.getConfiguration());
			disp=true; 
			procSamples=0;
			// ***** setup finished ***//
			System.out.println("Mapper_readRank setup finsihed! replicationNum_samp:"+replicationNum_samp);
			super.setup(context);
	 	}
		
		@Override
		protected void map(IntWritable key, V value, Context context) throws IOException, InterruptedException {
			//key: photoName, value: (ranked) docNames_scores	
			if (disp==true){ //debug disp info
				System.out.println("Mapper: read and replicate it to Key_QID_reRankFlagGroupID with "+replicationNum_samp+" times.");
				System.out.println("current key: "+key+", value: "+value);
				disp=false;
			}
			for (int i = 0; i < replicationNum_samp; i++) {
				context.write(new Key_RankFlagID_QID(i,key.get()), new Key_RankFlagID_QID(0,0));
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
	
}
