package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;

/**
 *
 *
 */
public class IntArr_byteArrArrArr implements Writable{
	//each element in byteArrArrArr should be equal length!
	int[] integers;
	byte[][][] byteArrArrArr; //use out.writeInt!! ,so byteArrArrArr[i].length need to be 0~2^31-1

	public IntArr_byteArrArrArr(int[] integers, byte[][][] byteArrArrArr) {
		super();
		assert integers.length == byteArrArrArr.length;
		int byteLength=byteArrArrArr[0][0].length;
		for(byte[][] onebyteArrArr: byteArrArrArr){//each element in byteArrArr should be equal length!
			for(byte[] onebyteArr:onebyteArrArr){
				assert byteLength == onebyteArr.length;
			}
		}
		this.integers = integers;
		this.byteArrArrArr = byteArrArrArr;
	}
	
	public IntArr_byteArrArrArr(HashMap<Integer,ArrayList<byte[]>> integer_byteArrList) {
		super();
		//save list to array
		int listLength=integer_byteArrList.keySet().size();
		int[] integers=new int[listLength];
		byte[][][] byteArrArrArr=new byte[listLength][][];
		int i=0;
		for(Integer integer : integer_byteArrList.keySet()){//
			//set int
			integers[i]=integer;
			//set byte[][]
			int byteArrList_length=integer_byteArrList.get(integer).size();
			byte[][] byteArrArr=new byte[byteArrList_length][];
			for(int j=0; j<byteArrList_length;j++){
				byte[] oneByteArr=integer_byteArrList.get(integer).get(j);
				byteArrArr[j]=oneByteArr;
			}
			byteArrArrArr[i]=byteArrArr;
			i++;
		}
		//check
		assert integers.length == byteArrArrArr.length;
		int byteLength=byteArrArrArr[0][0].length;
		for(byte[][] onebyteArrArr: byteArrArrArr){//each element in byteArrArr should be equal length!
			for(byte[] onebyteArr:onebyteArrArr){
				assert byteLength == onebyteArr.length;
			}
		}
		this.integers = integers;
		this.byteArrArrArr = byteArrArrArr;
	}
	
	/**
	 * 
	 */
	public IntArr_byteArrArrArr() {
		// do nothing
	}
	
	public void set(int[] integers, byte[][][] byteArrArrArr) {
		assert integers.length == byteArrArrArr.length;
		int byteLength=byteArrArrArr[0][0].length;
		for(byte[][] onebyteArrArr: byteArrArrArr){//each element in byteArrArr should be equal length!
			for(byte[] onebyteArr:onebyteArrArr){
				assert byteLength == onebyteArr.length;
			}
		}
		this.integers = integers;
		this.byteArrArrArr = byteArrArrArr;
	}

	public int[] getIntegers() {
		return integers;
	}

	public byte[][][] getbyteArrArrArr() {
		return byteArrArrArr;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		int list_length=integers.length;
		out.writeInt(list_length);
		int byteLength=byteArrArrArr[0][0].length;
		out.writeInt(byteLength);
		for(int i=0;i<list_length;i++){
			out.writeInt(integers[i]);
			int byteArrLength=byteArrArrArr[i].length; //as byteArrArrArr[i].length is usually quite small, so use out.writeShort!! ,so byteArrArrArr[i].length need to be 0~32767
			out.writeInt(byteArrLength);
			for(int j=0;j<byteArrLength;j++){
				out.write(byteArrArrArr[i][j]);
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int list_length = in.readInt();
		integers=new int[list_length];
		byteArrArrArr=new byte[list_length][][];
		int byteLength=in.readInt();
		for(int i=0; i<list_length; i++){
			//set int
			integers[i]=in.readInt();
			//set byte[][]
			int byteArrLength=in.readInt();
			byte[][] byteArrArr=new byte[byteArrLength][];
			//when byteArrs.add, it add oneByteArr's address not the value, so if not creat a new one, all element in byteArrs will be the same one as present oneByteArr
			for(int j=0;j<byteArrLength;j++){
				byte[] oneByteArr=new byte[byteLength];
				in.readFully(oneByteArr);
				byteArrArr[j]=oneByteArr;
			}
			byteArrArrArr[i]=byteArrArr;
		}
	}
	
}
