package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import MyAPI.Interface.FeatInd;
import MyAPI.Interface.I_HESig;

/**
 *  for hadoop key, it must implements WritableComparable, not just implements Writable for value!!
 */
public class HESig implements Writable, I_HESig, FeatInd {
	byte[] sig; //use short! out.writeShort(sig.length)! 
	short featInd;
	
	public HESig(byte[] sig, short featInd) {
		super();
		this.sig = sig;
		this.featInd = featInd;
	}
	
	public HESig(SURFfeat feat) {
		super();
		this.sig = feat.sig;
		this.featInd = feat.featInd;
	}
	
	public HESig() {
		this.sig = new byte[1];
		this.featInd = -1;
	}

	public void setSURFfeat(byte[] sig, short featInd) {
		this.sig = sig;
		this.featInd = featInd;
	}
	
	@Override
	public byte[] getHESig() {
		return sig;
	}
	
	@Override
	public int getFeatInd() {
		return featInd;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeShort(sig.length);
		out.write(sig);
		out.writeShort(featInd);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		sig = new byte[in.readShort()];
		in.readFully(sig);
		featInd = in.readShort();
	}

}
