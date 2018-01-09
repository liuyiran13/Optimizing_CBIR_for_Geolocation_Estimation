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
public class IntArr_byteArrArr implements Writable{
	//here sig use BitSet
	int[] integers;
	byte[][] byteArrArr; //each element in byteArrs should be equal length!

	public IntArr_byteArrArr(int[] integers, byte[][] byteArrArr) {
		super();
		assert integers.length == byteArrArr.length;
		int byteLength=byteArrArr[0].length;
		for(byte[] onebyteArr:byteArrArr){//each element in byteArrArr should be equal length!
			assert byteLength == onebyteArr.length;
		}
		this.integers = integers;
		this.byteArrArr = byteArrArr;
	}
	
	public IntArr_byteArrArr(ArrayList<Integer> integers_list, ArrayList<byte[]> byteArrArr_list) {
		super();
		//save list to array
		int listLength=integers_list.size();
		int[] integers=new int[listLength];
		byte[][] byteArrArr=new byte[listLength][];
		for(int i=0;i<listLength;i++){
			integers[i]=integers_list.get(i);
			byteArrArr[i]=byteArrArr_list.get(i);
		}
		//check
		assert integers.length == byteArrArr.length;
		int byteLength=byteArrArr[0].length;
		for(byte[] onebyteArr:byteArrArr){//each element in byteArrArr should be equal length!
			assert byteLength == onebyteArr.length;
		}
		this.integers = integers;
		this.byteArrArr = byteArrArr;
	}
	
	/**
	 * 
	 */
	public IntArr_byteArrArr() {
		// do nothing
	}
	
	public void set(int[] integers, byte[][] byteArrArr) {
		assert integers.length == byteArrArr.length;
		int byteLength=byteArrArr[0].length;
		for(byte[] onebyteArr:byteArrArr){//each element in byteArrArr should be equal length!
			assert byteLength == onebyteArr.length;
		}
		this.integers = integers;
		this.byteArrArr = byteArrArr;
	}

	public int[] getIntegers() {
		return integers;
	}

	public byte[][] getbyteArrArr() {
		return byteArrArr;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		int list_length=integers.length;
		out.writeInt(list_length);
		int byteLength=byteArrArr[0].length;
		out.writeInt(byteLength);
		for(int i=0;i<list_length;i++){
			out.writeInt(integers[i]);
			out.write(byteArrArr[i]);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int list_length = in.readInt();
		integers=new int[list_length];
		byteArrArr=new byte[list_length][];
		int byteLength=in.readInt();
		for(int i=0; i<list_length; i++){
			integers[i]=in.readInt();
			//when byteArrs.add, it add oneByteArr's address not the value, so if not creat a new one, all element in byteArrs will be the same one as present oneByteArr
			byte[] oneByteArr=new byte[byteLength];
			in.readFully(oneByteArr);
			byteArrArr[i]=oneByteArr;
		}
	}
	
}
