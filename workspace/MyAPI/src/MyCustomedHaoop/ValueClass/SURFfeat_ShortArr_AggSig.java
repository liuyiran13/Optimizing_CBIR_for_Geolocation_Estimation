package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

import MyAPI.Obj.DataInOutput_Functions;

public class SURFfeat_ShortArr_AggSig implements Writable {
	//use out.writeShort!! ,so feats.length need to be 0~32767
	public SURFfeat[] feats;//feats
	public byte[] aggSig;//aggregated representation for all feats assigned to the same vw.
	
	public SURFfeat_ShortArr_AggSig( SURFfeat[] feats, byte[] aggSig) {
		super();
		this.feats = feats;
		this.aggSig = aggSig;
	}
	
	public SURFfeat_ShortArr_AggSig(List<SURFfeat> feats, byte[] aggSig) {
		super();
		setArr(feats, aggSig);
	}
	
	/**
	 * 
	 */
	public SURFfeat_ShortArr_AggSig() {
		// do nothing
	}

	public void setArr(SURFfeat[] feats, byte[] aggSig) {
		this.feats = feats;
		this.aggSig = aggSig;
	}
	
	public void setArr(List<SURFfeat> ObjList, byte[] aggSig) {
		this.feats = ObjList.toArray(new SURFfeat[0]);
		this.aggSig = aggSig;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		//write out SURFfeat_List
		DataInOutput_Functions.writeShortSizeArr(feats, out);
		//write out aggSig
		DataInOutput_Functions.writeByteArr(aggSig, out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read SURFfeat_List
		feats=DataInOutput_Functions.readShortSizeArr(in, SURFfeat.class);
		//read aggSig
		aggSig=DataInOutput_Functions.readByteArr(in);
	}

}
