package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

/**
 *
 *
 */
public class IntList_FloatArrList implements Writable{
	ArrayList<Integer> integers;
	ArrayList<float[]> floatArrs; 

	public IntList_FloatArrList(ArrayList<Integer> integers, ArrayList<float[]> douArrs) {
		super();
		int douArrLength=douArrs.get(0).length;
		for(float[] oneDouArr:douArrs){//each element in byteArrs should be equal length!
			assert douArrLength == oneDouArr.length;
		}
		this.integers = integers;
		this.floatArrs = douArrs;
	}
	
	/**
	 * 
	 */
	public IntList_FloatArrList() {
		// do nothing

	}
	
	public ArrayList<Integer> getIntegers() {
		return integers;
	}
	
	public ArrayList<float[]> getFloatArrs() {
		return floatArrs;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		int intlist_length=integers.size();
		out.writeInt(intlist_length);
		for(int i=0;i<intlist_length;i++){
			out.writeInt(integers.get(i));
		}
		int floatList_Length=floatArrs.size();
		out.writeInt(floatList_Length);
		int floatArr_Length=floatArrs.get(0).length;
		out.writeInt(floatArr_Length);
		for(int i=0;i<floatList_Length;i++){
			for(int j=0;j<floatArr_Length;j++){
				out.writeFloat(floatArrs.get(i)[j]);
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int intlist_length=in.readInt();
		integers=new ArrayList<Integer>(intlist_length);
		for(int i=0;i<intlist_length;i++){
			integers.add(in.readInt());
		}
		int floatList_Length=in.readInt();
		floatArrs=new ArrayList<float[]>(floatList_Length);
		int floatArr_Length=in.readInt();
		for(int i=0;i<floatList_Length;i++){
			float[] floatArr=new float[floatArr_Length];
			for(int j=0;j<floatArr_Length;j++){
				floatArr[j]=in.readFloat();
			}
			floatArrs.add(floatArr);
		}
	}
	
}
