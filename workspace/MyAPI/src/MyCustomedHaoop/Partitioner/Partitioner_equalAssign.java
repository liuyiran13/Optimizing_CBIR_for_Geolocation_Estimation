package MyCustomedHaoop.Partitioner;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

import MyAPI.General.General;
import MyCustomedHaoop.KeyClass.PartitionKey;

public class Partitioner_equalAssign{

	int reducerInter;
	boolean isPartitionKey;
	
	public Partitioner_equalAssign(Configuration conf, boolean isPartitionKey) throws InterruptedException{
		if (conf.get("reducerInter")!=null) {
			reducerInter=Integer.valueOf(conf.get("reducerInter")); 
		} else {
			throw new InterruptedException("err! conf.get(\"reducerInter\") is null in Partitioner_equalAssign");
		}
		this.isPartitionKey=isPartitionKey;
	}
	
	public int getReducerNum(int sampleNum){
		return (sampleNum-1)/reducerInter+1;
	}
	
	@SuppressWarnings("rawtypes")
	public Class getPartitioner() {
		if(isPartitionKey){
			return Partitioner_equalAssign_keyFrom0_PartKey.class;
		}else{
			return Partitioner_equalAssign_keyFrom0.class;
		}
	}
	
	public static class Partitioner_equalAssign_keyFrom0 < V extends Writable> extends Partitioner<IntWritable, V>  implements Configurable{
		private Configuration conf;
		private int reducerInter; 
		
		/**If you want to configure your partitioner, implement org.apache.hadoop.confConfigurable and perform your setup in the setConf method*/
		@Override
	    public void setConf(Configuration configuration) {
			this.conf = configuration;
			reducerInter=Integer.valueOf(conf.get("reducerInter"));
		}
		
		@Override
		/**The getPartition method needs to return a number between 0 (inclusive) and the numOfReducers (exclusive)
		 * numPartitions is reducerNum,
		*/
		public int getPartition(IntWritable Key, V Value, int numPartitions) {
			int key=Key.get();
			int PartitionID=(int) (key/reducerInter);
			General.Assert(PartitionID<numPartitions, "PartitionID: "+PartitionID+", numPartitions: "+numPartitions);
			return PartitionID;
		}
	
		@Override
		public Configuration getConf() {
			return conf;
		}
	}
	
	public static class Partitioner_equalAssign_keyFrom0_PartKey <K extends PartitionKey, V extends Writable> extends Partitioner<K, V>  implements Configurable{
		private Configuration conf;
		private int reducerInter; 
		
		/**If you want to configure your partitioner, implement org.apache.hadoop.confConfigurable and perform your setup in the setConf method*/
		@Override
	    public void setConf(Configuration configuration) {
			this.conf = configuration;
			reducerInter=Integer.valueOf(conf.get("reducerInter"));
		}
		
		@Override
		/**The getPartition method needs to return a number between 0 (inclusive) and the numOfReducers (exclusive)
		 * numPartitions is reducerNum,
		*/
		public int getPartition(K Key, V Value, int numPartitions) {
			int key=Key.getPartitionKey();
			int PartitionID=(int) (key/reducerInter);
			General.Assert(PartitionID<numPartitions, "PartitionID: "+PartitionID+", numPartitions: "+numPartitions);
			return PartitionID;
		}
	
		@Override
		public Configuration getConf() {
			return conf;
		}
	}
	
}