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
public class Key_QID_DID implements WritableComparable,PartitionKey {
	public int queryID;
	public int docID;

	public Key_QID_DID(int queryID, int docID) {
		super();
		this.queryID = queryID;
		this.docID = docID;
	}
	
	/**
	 * 
	 */
	public Key_QID_DID() {
		// do nothing
	}

	public void set(int queryID, int docID) {
		this.queryID = queryID;
		this.docID = docID;
	}
	
	@Override
	public int getPartitionKey() {//use QueryID as partition key
		return queryID;
	}
	
	@Override 
	public String toString(){
		return "Q"+queryID+"_D"+docID;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(queryID);
		out.writeInt(docID);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		queryID=in.readInt();
		docID=in.readInt();
	}

	@Override
	public int compareTo(Object  o ) {//first compare based on the queryID
		Key_QID_DID that = (Key_QID_DID) o;
		if (this.queryID<that.queryID) {
			return -1;
		}else if (this.queryID>that.queryID) {
			return 1;
		}else {//second compare based on the docID
			if (this.docID<that.docID) {
				return -1;
			}else if (this.docID>that.docID) {
				return 1;
			}else {
				return 0;
			}
		}
	}
	
	@Override
	public int hashCode() {
        return queryID;
    }

}
