package MyCustomedHaoop.KeyClass;

import org.apache.hadoop.io.IntWritable;

/**
 *  for hadoop key, it must implements WritableComparable, not just implements Writable for value!!
 *
 */
public class Key_RankFlagID_QID extends AbstractTwoKey<IntWritable, IntWritable> implements PartitionKey, GroupKey<IntWritable>{	
	
	public Key_RankFlagID_QID(int rankFlagID, int queryID) {
		super(new IntWritable(rankFlagID), new IntWritable(queryID), IntWritable.class, IntWritable.class);
	}
	
	public Key_RankFlagID_QID() {
		super(IntWritable.class, IntWritable.class);
	}

	@Override
	public String toString(){
		return obj_1+"_"+obj_2;
	}
	
	@Override
	public int getPartitionKey() {//use rankFlagID as partition key
		return obj_1.get(); 
	}
	
	/**
	 * default: 1st key:RankFlagID as GroupKey
	 */
	@Override
	public IntWritable getGroupKey(){
		return obj_1;
	}
	
	
}
