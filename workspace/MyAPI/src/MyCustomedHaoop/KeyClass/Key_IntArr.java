package MyCustomedHaoop.KeyClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

/**
 *  for hadoop key, it must implements WritableComparable, not just implements Writable for value!!
 *
 */
@SuppressWarnings("rawtypes")
public class Key_IntArr implements WritableComparable {
	int[] IntArr;

	public Key_IntArr( int[] IntArr) {
		super();
		this.IntArr = IntArr;
	}
	
	/**
	 * 
	 */
	public Key_IntArr() {
		// do nothing
	}

	public void setFloatArr(int[] IntArr) {
		this.IntArr = IntArr;
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

	@Override
	public int compareTo(Object  o ) {// compare based on the fist value in the array
		int thisValue = this.IntArr[0];
		Key_IntArr that = (Key_IntArr) o;
		int thatValue=that.IntArr[0];
        return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
	}
	
	@Override
	public int hashCode() {
        return Arrays.hashCode(this.IntArr);
    }

}
