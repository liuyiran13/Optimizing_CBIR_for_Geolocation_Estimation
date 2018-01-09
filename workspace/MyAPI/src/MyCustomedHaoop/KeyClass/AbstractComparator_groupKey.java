package MyCustomedHaoop.KeyClass;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author xinchaoli
 */

public class AbstractComparator_groupKey <T extends Comparable<T>, K extends GroupKey<T> & WritableComparable<? super K>> extends WritableComparator {
		
	protected AbstractComparator_groupKey(Class<K> classK) {
        super(classK, true);
    }   
	
	@SuppressWarnings({ "rawtypes" })
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Key_RankFlagID_QID k1 = (Key_RankFlagID_QID)a;
		Key_RankFlagID_QID k2 = (Key_RankFlagID_QID)b;
        return k1.getGroupKey().compareTo(k2.getGroupKey());
    }
	
}


