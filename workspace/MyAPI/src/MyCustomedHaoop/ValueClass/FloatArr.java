package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 *
 */
public class FloatArr implements Writable{
	float[] FloatArr;

	public FloatArr( float[] floatArr) {
		super();
		this.FloatArr = floatArr;
	}
	
	/**
	 * 
	 */
	public FloatArr() {
		// do nothing
	}

	public void setFloatArr(float[] FloatArr) {
		this.FloatArr = FloatArr;
	}

	public float[] getFloatArr() {
		return FloatArr;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(FloatArr.length);
		for(int i=0;i<FloatArr.length;i++){
			out.writeFloat(FloatArr[i]);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int FloatArrLength=in.readInt();
		FloatArr=new float[FloatArrLength];

		for(int i=0;i<FloatArr.length;i++){
			FloatArr[i]=in.readFloat();
		}
	}
	

}
