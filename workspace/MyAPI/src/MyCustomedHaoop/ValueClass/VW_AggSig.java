package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import MyAPI.Obj.DataInOutput_Functions;


/**
 *  for hadoop key, it must implements WritableComparable, not just implements Writable for value!!
 */
public class VW_AggSig implements Writable {
	public int vw;
	public byte[] aggSig; //aggregate all feat (that assigned to same VW) to generate one byte representation 
	
	public VW_AggSig(int vw, byte[] aggSig) {
		super();
		this.vw = vw;
		this.aggSig = aggSig;
	}
	
	public VW_AggSig() {
		super();
		this.vw = -1;
		this.aggSig = null;
	}

	public void setSURFpoint(int vw, byte[] aggSig) {
		this.vw = vw;
		this.aggSig = aggSig;
	}
	
	public String toString() {
		return "vw:"+vw+", aggSig bit num:"+aggSig.length*8;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(vw);
		DataInOutput_Functions.writeByteArr(aggSig, out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		vw = in.readInt();
		aggSig=DataInOutput_Functions.readByteArr(in);
	}

}
