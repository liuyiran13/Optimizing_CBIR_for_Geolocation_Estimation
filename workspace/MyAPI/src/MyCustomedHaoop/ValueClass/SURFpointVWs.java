package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 *  for hadoop key, it must implements WritableComparable, not just implements Writable for value!!
 */
public class SURFpointVWs implements Writable {
	public SURFpoint point;
	public VW_Weight_HESig_ShortArr vws;
	
	public SURFpointVWs(SURFpoint point, VW_Weight_HESig_ShortArr vws) {
		super();
		this.point = point;
		this.vws = vws;
	}
	
	public SURFpointVWs() {
		super();
	}

	public void setSURFpoint(SURFpoint point, VW_Weight_HESig_ShortArr vws) {
		this.point = point;
		this.vws = vws;
	}
	
	public String toString() {
		return "point:"+point+", vws:"+vws;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		point.write(out);
		vws.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		point=new SURFpoint();
		point.readFields(in);
		vws=new VW_Weight_HESig_ShortArr();
		vws.readFields(in);
	}

}
