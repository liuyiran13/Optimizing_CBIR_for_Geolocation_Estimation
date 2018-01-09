package MyCustomedHaoop.KeyClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *  for hadoop key, it must implements WritableComparable, not just implements Writable for value!!
 *
 */
@SuppressWarnings("rawtypes")
public class Key_IntStr implements WritableComparable {
	int Int;
	String Str;

	public Key_IntStr( int Int, String Str) {
		super();
		this.Int = Int;
		this.Str = Str;
	}
	
	/**
	 * 
	 */
	public Key_IntStr() {
		// do nothing
	}

	public void setIntStr(int Int, String Str) {
		this.Int = Int;
		this.Str = Str;
	}

	public int getInt() {
		return Int;
	}
	
	public String getStr() {
		return Str;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		
		out.writeInt(Int);
		out.writeUTF(Str);
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		Int=in.readInt();
		Str=in.readUTF();
		
	}

	@Override
	public int compareTo(Object  o ) {// compare based on the fist value in the array
		int thisValue = this.Int;
		Key_IntStr that = (Key_IntStr) o;
		int thatValue=that.Int;
        return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
	}
	
	@Override
	public int hashCode() {
        return Int;
    }

}
