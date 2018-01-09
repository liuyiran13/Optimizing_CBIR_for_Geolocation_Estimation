package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *  for hadoop key, it must implements WritableComparable, not just implements Writable for value!!
 */
public class SURFfeat_noSig implements Writable {
	short featInd;
	SURFpoint point;
	
	public SURFfeat_noSig(short featInd, SURFpoint point) {
		super();
		this.featInd = featInd;
		this.point = point;		
	}
	
	public SURFfeat_noSig(SURFfeat surfFeat) {
		super();
		this.featInd = surfFeat.featInd;
		this.point = surfFeat.getSURFpoint();		
	}
	
	/**
	 * 
	 */
	public SURFfeat_noSig() {
		super();
		this.featInd = -1;
		this.point=new SURFpoint();
	}

	public void setSURFfeat(short featInd, SURFpoint point) {
		this.featInd = featInd;
		this.point = point;		
	}
	
	public short getFeatInd() {
		return featInd;
	}
	
	public SURFpoint getSURFpoint() {
		return point;
	}
	
	public String toString() {
		return "featInd:"+featInd+", "+point.toString();
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeShort(featInd);
		point.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		featInd = in.readShort();
		point=new SURFpoint();
		point.readFields(in);
	}

}
