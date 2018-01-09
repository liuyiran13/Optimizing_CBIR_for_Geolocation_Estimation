package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 *
 */
public class Int_Float implements Writable{
	public int integerV;
	public float floatV;

	public Int_Float(int integerV, float floatV) {
		super();
		this.integerV = integerV;
		this.floatV = floatV;
	}
	
	public Int_Float() {
		// do nothing
	}
	
	public void setIntFloat(int integerV, float floatV) {
		this.integerV = integerV;
		this.floatV = floatV;
	}

	public int getInt() {
		return integerV;
	}


	public float getfloat() {
		return floatV;
	}
	
	@Override
	public String toString(){
		return integerV+"_"+floatV;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(integerV);
		out.writeFloat(floatV);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		integerV = in.readInt();
		floatV = in.readFloat();
	}
	

}
