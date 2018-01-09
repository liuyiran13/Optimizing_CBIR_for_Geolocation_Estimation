package MyCustomedHaoop.Partitioner;

import java.util.Random;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

import MyCustomedHaoop.KeyClass.PartitionKey;

public class Partitioner_random_sameKey_PartKey <K extends PartitionKey,V extends Writable> extends Partitioner<K,V>  implements Configurable{
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
	public int getPartition(K Key, V Value, int numPartitions) {
		rand.setSeed(Key.getPartitionKey()); //use Key.queryID as the seed, guarantee same query value go to the same reducer!
		int PartitionID=rand.nextInt(numPartitions);
		return PartitionID;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}
}