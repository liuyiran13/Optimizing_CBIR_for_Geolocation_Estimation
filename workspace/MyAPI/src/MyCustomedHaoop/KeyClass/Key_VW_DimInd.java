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
public class Key_VW_DimInd implements WritableComparable,PartitionKey {
	public int vw;
	public int dimInd;
	
	
	public Key_VW_DimInd(int vw, int dimInd) {
		super();
		this.vw = vw;
		this.dimInd = dimInd;
	}
	
	/**
	 * 
	 */
	public Key_VW_DimInd() {
		// do nothing
	}

	public void set(int vw, int dimInd) {
		this.vw = vw;
		this.dimInd = dimInd;
	}

	@Override
	public int getPartitionKey() {//use vw as partition key
		return vw;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(vw);
		out.writeInt(dimInd);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		vw=in.readInt();
		dimInd=in.readInt();
	}
	
	@Override
	public String toString(){
		return vw+"_"+dimInd;
	}

	@Override
	public int compareTo(Object  o ) {// compare based on the vwin the array
		Key_VW_DimInd that = (Key_VW_DimInd) o;
		if (this.vw<that.vw) {
			return -1;
		}else if (this.vw>that.vw) {
			return 1;
		}else {//second compare based on the QueryID
			if (this.dimInd<that.dimInd) {
				return -1;
			}else if (this.dimInd>that.dimInd) {
				return 1;
			}else {
				return 0;
			}
		}
	}
	
	@Override
	public int hashCode() {
        return vw;
    }

}
