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
public class IntList implements Writable{
	ArrayList<Integer> integers;

	public IntList(ArrayList<Integer> integers) {
		super();
		this.integers = integers;
	}
	
	public IntList(List<Integer> integers) {
		super();
		this.integers = new ArrayList<Integer>(integers);
	}
	
	/**
	 * 
	 */
	public IntList() {
		// do nothing
	}
	
	public void set(ArrayList<Integer> integers) {
		this.integers = integers;
	}
	
	public ArrayList<Integer> getIntegers() {
		return integers;
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
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int list_length = in.readInt();
		integers=new ArrayList<Integer>(list_length);
		for(int i=0; i<list_length; i++){
			integers.add(in.readInt());
		}
	}
	
}
