package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 *  for hadoop key, it must implements WritableComparable, not just implements Writable for value!!
 */
public class VW_Weight_HESig implements Writable {
	public int vw;
	public float weight;
	public byte[] sig;
	
	public VW_Weight_HESig(int vw, float weight, byte[] sig) {
		super();
		this.vw = vw;
		this.weight = weight;
		this.sig = sig;
	}
	
	public VW_Weight_HESig() {
		super();
		this.vw = -1;
		this.weight = -1;
		this.sig = null;
	}

	public void setSURFpoint(int vw, float weight, byte[] sig) {
		this.vw = vw;
		this.weight = weight;
		this.sig = sig;
	}
	
	public String toString() {
		return "vw:"+vw+", weight:"+weight+", sig.length:"+sig.length;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(vw);
		out.writeFloat(weight);
		out.writeShort(sig.length);
		out.write(sig);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		vw = in.readInt();
		weight = in.readFloat();
		sig = new byte[in.readShort()];
		in.readFully(sig);
	}

}
