package MyCustomedHaoop.KeyClass;

import org.apache.hadoop.io.IntWritable;

/**
 * @author xinchaoli
 */

public class Comparator_groupKey_Collection{
	
	public static class Comparator_groupKey_Key_RankFlagID_QID  extends AbstractComparator_groupKey<IntWritable, Key_RankFlagID_QID> {
		
		protected Comparator_groupKey_Key_RankFlagID_QID() {
	        super(Key_RankFlagID_QID.class);
	    }   

	}
}

