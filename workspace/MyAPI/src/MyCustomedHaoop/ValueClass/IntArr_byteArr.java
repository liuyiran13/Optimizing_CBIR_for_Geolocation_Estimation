package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 *
 */
public class IntArr_byteArr implements Writable{
	int[]  IntArr;
	byte[] bytes;

	public IntArr_byteArr(int[] IntArr, byte[] bytes) {
		super();
		this.IntArr = IntArr;
		this.bytes = bytes;
	}
	
	/**
	 * 
	 */
	public IntArr_byteArr() {
		// do nothing
	}

	public void setIntArr_FloatArr(int[] IntArr, byte[] bytes) {
		this.IntArr = IntArr;
		this.bytes = bytes;
	}

	public int[] getIntArr() {
		return IntArr;
	}


	public byte[] getBytes() {
		return bytes;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(IntArr.length);
		
		out.writeInt(bytes.length);
		
		for(int i=0;i<IntArr.length;i++){
			out.writeInt(IntArr[i]);
		}
		
		out.write(bytes);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int IntArrLength=in.readInt();
		IntArr=new int[IntArrLength];
		
		int bytesLength=in.readInt();
		bytes=new byte[bytesLength];
		
		for(int i=0;i<IntArr.length;i++){
			IntArr[i]=in.readInt();
		}
		
		in.readFully(bytes);
	}
	

}
