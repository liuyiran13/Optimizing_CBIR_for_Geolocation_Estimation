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
public class Key_QID_VW implements WritableComparable,PartitionKey {
	public int queryID;
	public int vw;
	
	public Key_QID_VW(int queryID, int vw) {
		super();
		this.queryID = queryID;
		this.vw = vw;
	}
	
	/**
	 * 
	 */
	public Key_QID_VW() {
		// do nothing
	}

	public void set(int queryID, int vw) {
		this.queryID = queryID;
		this.vw = vw;
	}

	@Override
	public int getPartitionKey() {//use QueryID as partition key
		return queryID;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(queryID);
		out.writeInt(vw);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		queryID=in.readInt();
		vw=in.readInt();
	}

	@Override
	public int compareTo(Object  o ) {// compare based on the fist value in the array
		Key_QID_VW that = (Key_QID_VW) o;
		if (this.queryID<that.queryID) {
			return -1;
		}else if (this.queryID>that.queryID) {
			return 1;
		}else {
			return 0;
		}
	}
	
	@Override
	public int hashCode() {
        return queryID;
    }

}
