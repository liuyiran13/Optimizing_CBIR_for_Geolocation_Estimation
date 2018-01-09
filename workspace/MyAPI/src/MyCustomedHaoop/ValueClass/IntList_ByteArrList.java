package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

import MyAPI.General.General;

/**
 *
 *
 */
public class IntList_ByteArrList implements Writable{
	ArrayList<Integer> Ints;
	ArrayList<byte[]> byteArrs; //each element in byteArrs should be equal length!

	public IntList_ByteArrList(ArrayList<Integer> Ints, ArrayList<byte[]> byteArrs) {
		super();
		this.Ints = Ints;
		this.byteArrs = byteArrs;
		if(byteArrs.size()!=0){
			int byteArrLength=byteArrs.get(0).length;
			for(byte[] onebyteArr:byteArrs){//each element in byteArrs should be equal length!
				General.Assert(byteArrLength == onebyteArr.length, "byteArrLength="+byteArrLength+", onebyteArr.length="+onebyteArr.length) ;
			}
		}
	}
	
	public IntList_ByteArrList() {
		// do nothing
	}

	public void set(ArrayList<Integer> Ints, ArrayList<byte[]> byteArrs) {
		this.Ints = Ints;
		this.byteArrs = byteArrs;
		if(byteArrs.size()!=0){
			int byteArrLength=byteArrs.get(0).length;
			for(byte[] onebyteArr:byteArrs){//each element in byteArrs should be equal length!
				General.Assert(byteArrLength == onebyteArr.length, "byteArrLength="+byteArrLength+", onebyteArr.length="+onebyteArr.length) ;
			}
		}
	}
	
	public ArrayList<Integer> getInts() {
		return Ints;
	}
	

	public ArrayList<byte[]> getbyteArrs() {
		return byteArrs;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//write intList
		int intList_length=Ints.size();
		out.writeInt(intList_length);
		if(intList_length!=0){
			for(int i=0;i<intList_length;i++){
				out.writeInt(Ints.get(i));
			}
		}
		//write byteArrList
		int byteArrList_length=byteArrs.size();
		out.writeInt(byteArrList_length);
		if(byteArrList_length!=0){
			int byteArrLength=byteArrs.get(0).length;
			out.writeInt(byteArrLength);
			for(int i=0;i<byteArrList_length;i++){
				out.write(byteArrs.get(i));
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read intList
		int intList_length=in.readInt();
		Ints=new ArrayList<Integer>(intList_length);
		if(intList_length!=0){
			for(int i=0;i<intList_length;i++){
				Ints.add(in.readInt());
			}
		}
		//read byteArrList
		int byteArrList_length = in.readInt();
		byteArrs=new ArrayList<byte[]> (byteArrList_length);
		if(byteArrList_length!=0){
			int byteArrLength=in.readInt();
			for(int i=0; i<byteArrList_length; i++){
				//when byteArrs.add, it add oneByteArr's address not the value, so if not creat a new one, all element in byteArrs will be the same one as present oneByteArr
				byte[] oneByteArr=new byte[byteArrLength];
				in.readFully(oneByteArr);
				byteArrs.add(oneByteArr);
			}
		}
	}
	
}
