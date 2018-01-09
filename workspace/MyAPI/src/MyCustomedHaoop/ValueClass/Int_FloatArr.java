package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 *
 */
public class Int_FloatArr implements Writable{
	Integer integer;
	float[] Arr;

	public Int_FloatArr(Integer integer, float[] Arr) {
		super();
		this.integer = integer;
		this.Arr = Arr;
	}
	
	/**
	 * 
	 */
	public Int_FloatArr() {
		// do nothing
	}

	public void setIntegerByteArr(Integer integer, float[] Arr) {
		this.integer = integer;
		this.Arr = Arr;
	}

	public Integer getInteger() {
		return integer;
	}


	public float[] getArr() {
		return Arr;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(integer);
		out.writeInt(Arr.length);
		for (int i = 0; i < Arr.length; i++) {
			out.writeFloat(Arr[i]);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		integer = in.readInt();
		Arr = new float[in.readInt()];
		for (int i = 0; i < Arr.length; i++) {
			Arr[i]=in.readFloat();
		}
	}
	

}
