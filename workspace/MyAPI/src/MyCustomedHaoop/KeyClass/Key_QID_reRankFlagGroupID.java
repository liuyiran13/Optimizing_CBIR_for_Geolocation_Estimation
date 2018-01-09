package MyCustomedHaoop.KeyClass;

import org.apache.hadoop.io.IntWritable;


/**
 *  for hadoop key, it must implements WritableComparable, not just implements Writable for value!!
 *
 */
public class Key_QID_reRankFlagGroupID extends AbstractTwoKey<IntWritable, IntWritable> {	
	
	public Key_QID_reRankFlagGroupID(int queryID, int reRankFlagGroupID) {
		super(new IntWritable(queryID), new IntWritable(reRankFlagGroupID), IntWritable.class, IntWritable.class);
	}
	
	public Key_QID_reRankFlagGroupID() {
		super(IntWritable.class, IntWritable.class);
	}
	
}
