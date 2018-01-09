package MyCustomedHaoop.Partitioner;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partitioner_RemHash_KeyFrom0 <K extends IntWritable, V extends Writable> extends Partitioner<K,V>  implements Configurable{
	private Configuration conf;
	/**If you want to configure your partitioner, implement org.apache.hadoop.confConfigurable and perform your setup in the setConf method*/
	@Override
    public void setConf(Configuration configuration) {
		this.conf = configuration;	
	}
	
	@Override
	/**The getPartition method needs to return a number between 0 (inclusive) and the numOfReducers (exclusive)
	 * numPartitions is reducerNum,
	*/
	public int getPartition(K Key, V Value, int numPartitions) {
		int PartitionID=Key.get()%numPartitions;
		return PartitionID;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}
}