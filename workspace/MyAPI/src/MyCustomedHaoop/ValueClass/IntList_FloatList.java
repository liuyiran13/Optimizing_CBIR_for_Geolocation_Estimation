package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 *
 *
 */
public class IntList_FloatList implements Writable{
	ArrayList<Integer> integers;
	ArrayList<Float> floats; 

	public IntList_FloatList(ArrayList<Integer> integers, ArrayList<Float> floats) {
		super();
		this.integers = integers;
		this.floats = floats;
	}
	
	public IntList_FloatList(List<Integer> integers, List<Float> floats) {
		super();
		this.integers = new ArrayList<Integer>(integers);
		this.floats = new ArrayList<Float>(floats);
	}
	
	/**
	 * 
	 */
	public IntList_FloatList() {
		this.integers = new ArrayList<Integer>();
		this.floats = new ArrayList<Float>();
	}
	
	public void set(ArrayList<Integer> integers, ArrayList<Float> floats) {
		this.integers.clear(); 
		this.integers.addAll(integers);
		this.floats.clear(); 
		this.floats.addAll(floats);
	}
	
	public ArrayList<Integer> getIntegers() {
		return integers;
	}
	
	public ArrayList<Float> getFloats() {
		return floats;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		
		int list_length=integers.size();
		out.writeInt(list_length);
		for(int i=0;i<list_length;i++){
			out.writeInt(integers.get(i));
		}
		
		list_length=floats.size();
		out.writeInt(list_length);
		for(int i=0;i<list_length;i++){
			out.writeFloat(floats.get(i));
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int list_length = in.readInt();
		integers=new ArrayList<Integer>(2*list_length);
		for(int i=0; i<list_length; i++){
			integers.add(in.readInt());
		}
		
		list_length = in.readInt();
		floats=new ArrayList<Float>(2*list_length);
		for(int i=0; i<list_length; i++){
			floats.add(in.readFloat());
		}
	}
	
}
