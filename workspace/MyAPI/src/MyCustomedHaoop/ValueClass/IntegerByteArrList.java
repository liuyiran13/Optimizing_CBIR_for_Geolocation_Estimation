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
public class IntegerByteArrList implements Writable{
	//here sig use BitSet
	ArrayList<Integer> integers;
	ArrayList<byte[]> byteArrs; //each element in byteArrs should be equal length!

	public IntegerByteArrList(ArrayList<Integer> integers, ArrayList<byte[]> byteArrs) {
		super();
		assert integers.size() == byteArrs.size();
		this.integers = integers;
		this.byteArrs = byteArrs;
		int byteArrLength=byteArrs.get(0).length;
		for(byte[] onebyteArr:byteArrs){//each element in byteArrs should be equal length!
			assert byteArrLength == onebyteArr.length;
		}
	}
	
	/**
	 * 
	 */
	public IntegerByteArrList() {
		// do nothing
		integers=new ArrayList<Integer>();
		byteArrs=new ArrayList<byte[]>();
	}

	public ArrayList<Integer> getIntegers() {
		return integers;
	}

	public ArrayList<byte[]> getbyteArrs() {
		return byteArrs;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		int list_length=integers.size();
		out.writeInt(list_length);
		int byteArrLength=byteArrs.get(0).length;
		out.writeInt(byteArrLength);
		for(int i=0;i<list_length;i++){
			out.writeInt(integers.get(i));
			out.write(byteArrs.get(i));
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int list_length = in.readInt();
		integers=new ArrayList<Integer>(list_length);
		byteArrs=new ArrayList<byte[]> (list_length);
		int byteArrLength=in.readInt();
		for(int i=0; i<list_length; i++){
			integers.add(in.readInt());
			//when byteArrs.add, it add oneByteArr's address not the value, so if not creat a new one, all element in byteArrs will be the same one as present oneByteArr
			byte[] oneByteArr=new byte[byteArrLength];
			in.readFully(oneByteArr);
			byteArrs.add(oneByteArr);
		}
	}
	
}
