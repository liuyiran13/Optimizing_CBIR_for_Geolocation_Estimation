package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;

import org.apache.hadoop.io.Writable;

public abstract class AbstractArrWritable <T extends Writable>  implements Writable {
	//use out.writeInt!! ,so ObjArr.length need to be 0~2^31-1
	T[] ObjArr;
	private Class<T> clazz;

	public AbstractArrWritable(T[] ObjArr, Class<T> clazz){
		super();
		this.ObjArr = ObjArr;
		this.clazz = clazz;
	}
	
	public AbstractArrWritable(List<T> ObjList, Class<T> clazz) {
		super();
		this.clazz = clazz;
		setArr(ObjList);
	}
	
	/**
	 * 
	 */
	public AbstractArrWritable(Class<T> clazz) {//within Hadoop map-reduce, it needs a no-argument constractor, so this class cannot be directly used in hadoop!
		this.clazz = clazz;
	}

	public void setArr(T[] ObjArr) {
		this.ObjArr = ObjArr;
	}
	
	@SuppressWarnings("unchecked")
	public void setArr(List<T> ObjList) {
		T[] array = (T[]) Array.newInstance(clazz, 0);
		this.ObjArr = ObjList.toArray(array);
	}

	public T[] getArr() {
		return ObjArr;
	}

	@Override
	public String toString(){
		return "element num: "+ObjArr.length;
	} 

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(ObjArr.length);
		for(int i=0;i<ObjArr.length;i++){
			ObjArr[i].write(out);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int ArrLength=in.readInt();
		ObjArr=(T[]) Array.newInstance(clazz, ArrLength);
		try {
			for(int i=0;i<ArrLength;i++){
				ObjArr[i]=clazz.newInstance();
				ObjArr[i].readFields(in);
			}
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
			throw new IOException("InstantiationException or IllegalAccessException in WritableArr");
		}
	}

}
