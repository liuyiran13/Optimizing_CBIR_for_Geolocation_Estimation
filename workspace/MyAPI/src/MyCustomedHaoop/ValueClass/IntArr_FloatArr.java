package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 *
 *
 */
public class IntArr_FloatArr implements Writable{
	int[]  intArr;
	List<Integer> intList; //it allows to use arrayList when save
	float[] floatArr;
	List<Float> floatList;

	public IntArr_FloatArr(int[] IntArr, float[] FloatArr) {
		super();
		this.intArr = IntArr;
		this.floatArr = FloatArr;
	}
	
	public IntArr_FloatArr(List<Integer> intList, float[] FloatArr) {
		super();
		this.intList = intList;
		this.floatArr = FloatArr;
	}
	
	public IntArr_FloatArr(List<Integer> intList, List<Float> floatList) {
		super();
		this.intList = intList;
		this.floatList = floatList;
	}
	
	public IntArr_FloatArr(List<Int_Float> list) {
		super();
		intArr=new int[list.size()];
		floatArr=new float[list.size()];
		int i=0;
		for (Int_Float int_Float : list) {
			intArr[i]=int_Float.integerV;
			floatArr[i]=int_Float.floatV;
			i++;
		}
	}
	
	/**
	 * 
	 */
	public IntArr_FloatArr() {
		// do nothing
	}

	public void setIntArr_FloatArr(int[] intArr, float[] FloatArr) {
		this.intArr = intArr;
		this.floatArr = FloatArr;
	}

	public int[] getIntArr() {
		return intArr;
	}


	public float[] getFloatArr() {
		return floatArr;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(intArr==null?intList.size():intArr.length);
		
		out.writeInt(floatArr==null?floatList.size():floatArr.length);
		
		if (intArr==null) {//use intList instead
			for(int one:intList){
				out.writeInt(one);
			}
		}else {
			for(int one:intArr){
				out.writeInt(one);
			}
		}
		if (floatArr==null) {//use intList instead
			for(float one:floatList){
				out.writeFloat(one);
			}
		}else {
			for(float one:floatArr){
				out.writeFloat(one);
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int IntArrLength=in.readInt();
		intArr=new int[IntArrLength];
		
		int FloatArrLength=in.readInt();
		floatArr=new float[FloatArrLength];
		
		for(int i=0;i<intArr.length;i++){
			intArr[i]=in.readInt();
		}
		
		for(int i=0;i<floatArr.length;i++){
			floatArr[i]=in.readFloat();
		}
	}
	

}
