package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import MyAPI.General.General;
import MyAPI.Interface.FeatInd;
import MyAPI.Interface.I_HESig;

/**
 *  for hadoop key, it must implements WritableComparable, not just implements Writable for value!!
 */
public class SURFfeat extends SURFpoint implements Writable,I_HESig,FeatInd {
	byte[] sig; //use short! out.writeShort(sig.length)! 
	short featInd;
	
	public SURFfeat(byte[] sig, short featInd, SURFpoint point) {
		super(point);
		this.sig = sig;
		this.featInd = featInd;
	}
	
	public SURFfeat() {
		super();
		this.sig = new byte[1];
		this.featInd = -1;
	}
	
	public String toString() {
		return "featInd:"+featInd+", sig:"+General.ByteArraytoBitSet(sig)+", SURFpoint:"+super.toString();
	}

	public void setSURFfeat(byte[] sig, short featInd, SURFpoint point) {
		this.sig = sig;
		this.featInd = featInd;
		super.setSURFpoint(point);
	}
	
	@Override
	public byte[] getHESig() {
		return sig;
	}
	
	@Override
	public int getFeatInd() {
		return featInd;
	}
	
	public HESig getHESigFull(){
		return new HESig(sig, featInd);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeShort(sig.length);
		out.write(sig);
		out.writeShort(featInd);
		super.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		sig = new byte[in.readShort()];
		in.readFully(sig);
		featInd = in.readShort();
		super.readFields(in);
	}

	

}
