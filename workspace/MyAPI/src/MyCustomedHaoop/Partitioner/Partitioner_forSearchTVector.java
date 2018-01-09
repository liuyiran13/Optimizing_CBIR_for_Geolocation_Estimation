package MyCustomedHaoop.Partitioner;

import java.util.Random;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.imagR.Conf_ImageR;

public class Partitioner_forSearchTVector <V extends Writable> extends Partitioner<IntWritable,V>  implements Configurable{
	private Configuration conf;
	private int[] PaIDs; //save vw's last group's PartitionID
	private Random rand ;  
	/**If you want to configure your partitioner, implement org.apache.hadoop.confConfigurable and perform your setup in the setConf method*/
	
    public void setConf(Configuration configuration) {
    	this.conf = configuration;
		//** set PaIDs **//
		try {
			PaIDs= (int[]) General.readObject(Conf_ImageR.sd_VWPaIDs);//each element in PaIDs is mutipled by 10!
		} catch (InterruptedException e) {
			System.err.println("error in Partitioner_VW, load PaIDs from distributed cache fail, IOException!! ");
			e.printStackTrace();
		}
		System.out.println("Partitioner_VW: PaIDs set finished, total partioned reducer number : "+General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs)+", job.setNumReduceTasks(jobRedNum) should >= this value!!");
		//** set Random generator **//
		rand= new Random();

		System.out.println("Partitioner setup finished!!");
	}
	
	@Override
	/**The getPartition method needs to return a number between 0 (inclusive) and the numOfReducers (exclusive)
	 * numPartitions is reducerNum, should be larger than 
	*/
	public int getPartition(IntWritable Key, V Value, int numPartitions) {
		return General_Hadoop.getVWPartitionID_from_vwPartitionIDs(Key.get(), PaIDs, numPartitions, rand);
	}

	@Override
	public Configuration getConf() {
		return conf;
	}
}