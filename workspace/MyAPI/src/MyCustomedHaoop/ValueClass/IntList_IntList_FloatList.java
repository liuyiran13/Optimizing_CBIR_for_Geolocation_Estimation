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
public class IntList_IntList_FloatList implements Writable{
	ArrayList<Integer> integers1;
	ArrayList<Integer> integers2;
	ArrayList<Float> floats; //each element in integers1, integers2, floats should be equal length!

	public IntList_IntList_FloatList(ArrayList<Integer> integers1,ArrayList<Integer> integers2, ArrayList<Float> floats) {
		super();
		assert integers1.size() == integers2.size();
		assert floats.size() == integers2.size();
		this.integers1 = integers1;
		this.integers2 = integers2;
		this.floats = floats;
	}
	
	/**
	 * 
	 */
	public IntList_IntList_FloatList() {
		// do nothing

	}

	public ArrayList<Integer> getIntegers1() {
		return integers1;
	}
	
	public ArrayList<Integer> getIntegers2() {
		return integers2;
	}
	
	public ArrayList<Float> getFloats() {
		return floats;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		int list_length=integers1.size();
		out.writeInt(list_length);
		for(int i=0;i<list_length;i++){
			out.writeInt(integers1.get(i));
			out.writeInt(integers2.get(i));
			out.writeFloat(floats.get(i));
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int list_length = in.readInt();
		integers1=new ArrayList<Integer>(list_length);
		integers2=new ArrayList<Integer>(list_length);
		floats=new ArrayList<Float>(list_length);
		for(int i=0; i<list_length; i++){
			integers1.add(in.readInt());
			integers2.add(in.readInt());
			floats.add(in.readFloat());
		}
	}
	
}
