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
public class Int_ByteArrList implements Writable{
	int Int;
	ArrayList<byte[]> byteArrs; //each element in byteArrs should be equal length!

	public Int_ByteArrList(int Int, ArrayList<byte[]> byteArrs) {
		super();
		this.Int = Int;
		this.byteArrs = byteArrs;
		int byteArrLength=byteArrs.get(0).length;
		for(byte[] onebyteArr:byteArrs){//each element in byteArrs should be equal length!
			assert byteArrLength == onebyteArr.length;
		}
	}
	
	/**
	 * 
	 */
	public Int_ByteArrList() {
		// do nothing
	}

	public void set(int Int, ArrayList<byte[]> byteArrs) {
		this.Int = Int;
		this.byteArrs = byteArrs;
		int byteArrLength=byteArrs.get(0).length;
		for(byte[] onebyteArr:byteArrs){//each element in byteArrs should be equal length!
			assert byteArrLength == onebyteArr.length;
		}
	}
	
	public int getInt() {
		return Int;
	}
	

	public ArrayList<byte[]> getbyteArrs() {
		return byteArrs;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(Int);
		int list_length=byteArrs.size();
		out.writeInt(list_length);
		int byteArrLength=byteArrs.get(0).length;
		out.writeInt(byteArrLength);
		for(int i=0;i<list_length;i++){
			out.write(byteArrs.get(i));
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		Int=in.readInt();
		int list_length = in.readInt();
		byteArrs=new ArrayList<byte[]> (list_length);
		int byteArrLength=in.readInt();
		for(int i=0; i<list_length; i++){
			//when byteArrs.add, it add oneByteArr's address not the value, so if not creat a new one, all element in byteArrs will be the same one as present oneByteArr
			byte[] oneByteArr=new byte[byteArrLength];
			in.readFully(oneByteArr);
			byteArrs.add(oneByteArr);
		}
	}
	
}
