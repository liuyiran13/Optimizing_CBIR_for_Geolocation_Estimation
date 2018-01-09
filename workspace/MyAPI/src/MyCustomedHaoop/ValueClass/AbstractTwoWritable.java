package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public abstract class AbstractTwoWritable <T extends Writable, V extends Writable>  implements Writable {
	//use out.writeInt!! ,so ObjArr.length need to be 0~2^31-1
	public T obj_1;
	public V obj_2;
	private Class<T> clazz_T;
	private Class<V> clazz_V;

	public AbstractTwoWritable(T obj_T, V obj_V, Class<T> clazz_T, Class<V> clazz_V){
		super();
		this.obj_1 = obj_T;
		this.obj_2 = obj_V;
		this.clazz_T = clazz_T;
		this.clazz_V = clazz_V;
	}
	
	/**
	 * 
	 */
	public AbstractTwoWritable(Class<T> clazz_T, Class<V> clazz_V) {//within Hadoop map-reduce, it needs a no-argument constractor, so this class cannot be directly used in hadoop!
		this.clazz_T = clazz_T;
		this.clazz_V = clazz_V;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		obj_1.write(out);
		obj_2.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		try {
			//read obj_T
			obj_1=clazz_T.newInstance();
			obj_1.readFields(in);
			//read obj_V
			obj_2=clazz_V.newInstance();
			obj_2.readFields(in);
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
			throw new IOException("InstantiationException or IllegalAccessException in AbstractTwoWritable");
		}
	}

}
