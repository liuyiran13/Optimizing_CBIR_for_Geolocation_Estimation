package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class SURFfeat_noSig_ShortArr implements Writable {
	//use out.writeShort!! ,so ObjArr.length need to be 0~32767
	SURFfeat_noSig[] ObjArr;

	public SURFfeat_noSig_ShortArr( SURFfeat_noSig[] ObjArr) {
		super();
		this.ObjArr = ObjArr;
	}
	
	public SURFfeat_noSig_ShortArr(SURFfeat[] ObjArr) {
		super();
		this.ObjArr = new SURFfeat_noSig[ObjArr.length];
		for (int i = 0; i < ObjArr.length; i++) {
			this.ObjArr[i]= new SURFfeat_noSig(ObjArr[i]);
		}
	}
	
	public SURFfeat_noSig_ShortArr( ArrayList<SURFfeat_noSig> ObjList) {
		super();
		setArr(ObjList);
	}
	
	/**
	 * 
	 */
	public SURFfeat_noSig_ShortArr() {
		// do nothing
	}

	public void setArr(SURFfeat_noSig[] ObjArr) {
		this.ObjArr = ObjArr;
	}
	
	public void setArr(ArrayList<SURFfeat_noSig> ObjList) {
		this.ObjArr = ObjList.toArray(new SURFfeat_noSig[0]);
	}

	public SURFfeat_noSig[] getArr() {
		return ObjArr;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeShort(ObjArr.length);
		for(int i=0;i<ObjArr.length;i++){
			ObjArr[i].write(out);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int ArrLength=in.readShort();
		ObjArr=new SURFfeat_noSig[ArrLength];
		for(int i=0;i<ArrLength;i++){
			ObjArr[i]=new SURFfeat_noSig();
			ObjArr[i].readFields(in);
		}
	}

}
