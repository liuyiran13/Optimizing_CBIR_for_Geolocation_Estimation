package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

import MyAPI.Obj.DataInOutput_Functions;

public class SURFfeat_ShortArr_Arr implements Writable {
	//use SURFfeat_ShortArr!! ,so ObjArrArr[i]=SURFfeat_ShortArr, its length need to be 0~32767
	SURFfeat_ShortArr_AggSig[] ObjArrArr;

	public SURFfeat_ShortArr_Arr( SURFfeat[][] ObjArrArr, byte[][] aggSigs) {
		super();
		this.ObjArrArr = new SURFfeat_ShortArr_AggSig[ObjArrArr.length];
		for (int i = 0; i < ObjArrArr.length; i++) {
			this.ObjArrArr[i]=new SURFfeat_ShortArr_AggSig(ObjArrArr[i], aggSigs[i]);
		}
	}
	
	public SURFfeat_ShortArr_Arr(List<SURFfeat_ShortArr_AggSig> ObjArrList) {
		super();
		setArrArr(ObjArrList);
	}
	
	/**
	 * 
	 */
	public SURFfeat_ShortArr_Arr() {
		// do nothing
	}

	public void setArrArr(SURFfeat[][] ObjArrArr, byte[][] aggSigs) {
		this.ObjArrArr = new SURFfeat_ShortArr_AggSig[ObjArrArr.length];
		for (int i = 0; i < ObjArrArr.length; i++) {
			this.ObjArrArr[i]=new SURFfeat_ShortArr_AggSig(ObjArrArr[i], aggSigs[i]);
		}
	}
	
	public void setArrArr(List<SURFfeat_ShortArr_AggSig> ObjArrList) {
		ObjArrArr=ObjArrList.toArray(new SURFfeat_ShortArr_AggSig[0]);
	}

	public SURFfeat_ShortArr_AggSig[] getArrArr() {
		return ObjArrArr;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		DataInOutput_Functions.writeArr(ObjArrArr, out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		ObjArrArr=DataInOutput_Functions.readArr(in, SURFfeat_ShortArr_AggSig.class);
		
	}

}
