package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 *  for hadoop key, it must implements WritableComparable, not just implements Writable for value!!
 *
 */
public class IntArr implements Writable {
	private int[] IntArr;

	public IntArr( int size) {
		this.IntArr = new int[size];
	}
	
	public IntArr( int[] IntArr) {
		this.IntArr = IntArr;
	}
	
	public IntArr( List<Integer> integersList) {
		setIntArr(integersList);
	}
	
	/**
	 * 
	 */
	public IntArr() {
		// do nothing
	}

	public void setIntArr(int[] IntArr) {
		this.IntArr = IntArr;
	}
	
	public void setIntArr(List<Integer> integersList) {
		int[] integers=new int[integersList.size()];
		//make integers
		int i=0;
		for (int one : integersList) {
			integers[i]=one;
			i++;
		}
		this.IntArr = integers;
	}

	public int[] getIntArr() {
		return IntArr;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(IntArr.length);
		for(int i=0;i<IntArr.length;i++){
			out.writeInt(IntArr[i]);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int IntArrLength=in.readInt();
		IntArr=new int[IntArrLength];

		for(int i=0;i<IntArr.length;i++){
			IntArr[i]=in.readInt();
		}
	}

}
