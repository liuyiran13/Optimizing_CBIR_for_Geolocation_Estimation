package MyCustomedHaoop.Partitioner;

import java.util.Random;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partitioner_random_sameKey <V extends Writable> extends Partitioner<IntWritable,V>  implements Configurable{
	private Configuration conf;
	private Random rand ;  
	/**If you want to configure your partitioner, implement org.apache.hadoop.confConfigurable and perform your setup in the setConf method*/
	@Override
    public void setConf(Configuration configuration) {
		this.conf = configuration;
		//** set Random generator **//
		rand= new Random();			
	}
	
	@Override
	/**The getPartition method needs to return a number between 0 (inclusive) and the numOfReducers (exclusive)
	 * numPartitions is reducerNum,
	*/
	public int getPartition(IntWritable Key, V Value, int numPartitions) {
		rand.setSeed(Key.get()); //use Key as the seed, guarantee same key value go to the same reducer!
		int PartitionID=rand.nextInt(numPartitions);
		return PartitionID;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}
}