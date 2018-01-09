package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

import MyAPI.Obj.DataInOutput_Functions;

public class HESig_ShortArr_AggSig implements Writable {
	//use out.writeShort!! ,so ObjArr.length need to be 0~32767
	public HESig[] HESigs;
	public byte[] aggSig;//aggregated representation for all feats assigned to the same vw.

	public HESig_ShortArr_AggSig( HESig[] HESigs, byte[] aggSig) {
		super();
		this.HESigs = HESigs;
		this.aggSig = aggSig;
	}
	
	public HESig_ShortArr_AggSig( ArrayList<HESig> HESigs, byte[] aggSig) {
		super();
		setArr(HESigs, aggSig);
	}
	
	/**
	 * 
	 */
	public HESig_ShortArr_AggSig() {
		// do nothing
	}

	public void setArr(HESig[] HESigs, byte[] aggSig) {
		this.HESigs = HESigs;
		this.aggSig = aggSig;
	}
	
	public void setArr(ArrayList<HESig> HESigs, byte[] aggSig) {
		this.HESigs = HESigs.toArray(new HESig[0]);
		this.aggSig = aggSig;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {	
		//write out SURFfeat_List
		DataInOutput_Functions.writeShortSizeArr(HESigs, out);
		//write out aggSig
		DataInOutput_Functions.writeByteArr(aggSig, out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read SURFfeat_List
		HESigs=DataInOutput_Functions.readShortSizeArr(in, HESig.class);
		//read aggSig
		aggSig=DataInOutput_Functions.readByteArr(in);
	}

}
