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
public class IntListList_FloatListList implements Writable{
	ArrayList<ArrayList<Integer>> integers;
	ArrayList<ArrayList<Float>> floats; 

	public IntListList_FloatListList(ArrayList<ArrayList<Integer>> integers, ArrayList<ArrayList<Float>> floats) {
		super();
		this.integers = integers;
		this.floats = floats;
	}
	
	/**
	 * 
	 */
	public IntListList_FloatListList() {
		// do nothing

	}
	
	public ArrayList<ArrayList<Integer>> getIntegers() {
		return integers;
	}
	
	public ArrayList<ArrayList<Float>> getFloats() {
		return floats;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		int list0_length=integers.size();
		out.writeInt(list0_length);
		for(int List_i=0;List_i<list0_length;List_i++){
			int list1_length=integers.get(List_i).size();
			out.writeInt(list1_length);
			for(int i=0;i<list1_length;i++){
				out.writeInt(integers.get(List_i).get(i));
			}
		}
		
		list0_length=floats.size();
		out.writeInt(list0_length);
		for(int List_i=0;List_i<list0_length;List_i++){
			int list1_length=floats.get(List_i).size();
			out.writeInt(list1_length);
			for(int i=0;i<list1_length;i++){
				out.writeFloat(floats.get(List_i).get(i));
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int list0_length = in.readInt();
		integers=new ArrayList<ArrayList<Integer>>(list0_length);
		for(int List_i=0;List_i<list0_length;List_i++){
			int list1_length=in.readInt();
			ArrayList<Integer> oneList=new ArrayList<Integer>(list1_length);
			for(int i=0; i<list1_length; i++){
				oneList.add(in.readInt());
			}
			integers.add(oneList);
		}
		
		list0_length = in.readInt();
		floats=new ArrayList<ArrayList<Float>>(list0_length);
		for(int List_i=0;List_i<list0_length;List_i++){
			int list1_length=in.readInt();
			ArrayList<Float> oneList=new ArrayList<Float>(list1_length);
			for(int i=0; i<list1_length; i++){
				oneList.add(in.readFloat());
			}
			floats.add(oneList);
		}
	}
	
}
