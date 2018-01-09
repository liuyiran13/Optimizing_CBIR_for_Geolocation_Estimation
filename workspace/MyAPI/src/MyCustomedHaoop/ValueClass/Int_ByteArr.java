package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 *
 */
public class Int_ByteArr implements Writable{
	Integer integer;
	byte[] byteArr;

	public Int_ByteArr(Integer integer, byte[] byteArr) {
		super();
		this.integer = integer;
		this.byteArr = byteArr;
	}
	
	/**
	 * 
	 */
	public Int_ByteArr() {
		// do nothing
	}

	public void setIntegerByteArr(Integer integer, byte[] byteArr) {
		this.integer = integer;
		this.byteArr = byteArr;
	}

	public Integer getInteger() {
		return integer;
	}


	public byte[] getByteArr() {
		return byteArr;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(integer);
		out.writeInt(byteArr.length);
		out.write(byteArr);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		integer = in.readInt();
		byteArr = new byte[in.readInt()];
		in.readFully(byteArr);
	}
	

}
