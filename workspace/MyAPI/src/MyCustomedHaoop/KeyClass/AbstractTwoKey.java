package MyCustomedHaoop.KeyClass;

import org.apache.hadoop.io.WritableComparable;

import MyCustomedHaoop.ValueClass.AbstractTwoWritable;

public abstract class AbstractTwoKey <K1 extends WritableComparable<K1>, K2 extends WritableComparable<K2>> extends AbstractTwoWritable<K1,K2> implements WritableComparable<AbstractTwoKey<K1,K2>> {

	public AbstractTwoKey(K1 key1, K2 key2, Class<K1> classK1, Class<K2> classK2){
		super(key1, key2, classK1, classK2);
	}
	
	public AbstractTwoKey(Class<K1> clazz_T, Class<K2> clazz_V) {//within Hadoop map-reduce, it needs a no-argument constractor, so this class cannot be directly used in hadoop!
		super(clazz_T, clazz_V);
	}
	
	/**
	 * default: 1st key's hashCode
	 * Guarantee same key get same hashCode(otherwise it is disaster!), but not necessary that different key has different hashCode, as this only affect the efficiency
	 * The default partitioner in Hadoop is the HashPartitioner, which uses the hashCode method to determine which reducer to send the K,V pair to.
	 */
	@Override
	public int hashCode() {
        return obj_1.hashCode();
    }
	
	/**
	 * default: first compare 1st key, and then 2nd key, both in T, V's default compareTo order.
	 */
	@Override
	public int compareTo(AbstractTwoKey<K1,K2> that) {
		int res=this.obj_1.compareTo(that.obj_1);
		if (res==0) {
			res=this.obj_2.compareTo(that.obj_2);
		}
		return res;
	}
		
}
