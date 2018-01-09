package MyCustomedHaoop.KeyClass;

import org.apache.hadoop.io.IntWritable;

/**
 *  for hadoop key, it must implements WritableComparable, not just implements Writable for value!!
 *
 */
public class IntWritable_hasPartitionKey extends IntWritable implements PartitionKey {
	
	public IntWritable_hasPartitionKey(int Int) {
		super(Int);
	}

	@Override
	public int getPartitionKey() {//use int value as partition key
		return this.get();
	}
	
}
