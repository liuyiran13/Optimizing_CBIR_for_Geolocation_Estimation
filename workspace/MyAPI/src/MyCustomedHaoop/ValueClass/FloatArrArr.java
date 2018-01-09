package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class FloatArrArr implements Writable{
	FloatArr[] ObjArrArr;

	public FloatArrArr( float[][] ObjArrArr) {
		super();
		this.ObjArrArr = new FloatArr[ObjArrArr.length];
		for (int i = 0; i < ObjArrArr.length; i++) {
			this.ObjArrArr[i]=new FloatArr(ObjArrArr[i]);
		}
	}
	
	public FloatArrArr(List<float[]> ObjArrList) {
		super();
		setArrArr(ObjArrList);
	}
	
	public FloatArrArr() {
		// do nothing
	}

	public void setArrArr(float[][] ObjArrArr) {
		this.ObjArrArr = new FloatArr[ObjArrArr.length];
		for (int i = 0; i < ObjArrArr.length; i++) {
			this.ObjArrArr[i]=new FloatArr(ObjArrArr[i]);
		}
	}
	
	public void setArrArr(List<float[]> ObjArrList) {
		ObjArrArr = new FloatArr[ObjArrList.size()];
		int i = 0;
		for (float[] one:ObjArrList) {
			ObjArrArr[i]=new FloatArr(one);
			i++;
		}
	}

	public float[][] getArrArr() {
		float[][] arrs=new float[ObjArrArr.length][];
		for (int i = 0; i < arrs.length; i++) {
			arrs[i]=ObjArrArr[i].getFloatArr();
		}
		return arrs;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(ObjArrArr.length);
		for(int i=0;i<ObjArrArr.length;i++){
			ObjArrArr[i].write(out);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int ArrArrLength=in.readInt();
		ObjArrArr=new FloatArr[ArrArrLength];
		for(int i=0;i<ArrArrLength;i++){
			ObjArrArr[i]=new FloatArr();
			ObjArrArr[i].readFields(in);
		}
	}
	

}
