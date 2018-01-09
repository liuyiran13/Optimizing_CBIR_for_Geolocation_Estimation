package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;

public class VW_Weight_HESig_ShortArr implements Writable {
	//use out.writeShort!! ,so ObjArr.length need to be 0~32767
	VW_Weight_HESig[] ObjArr;

	public VW_Weight_HESig_ShortArr( VW_Weight_HESig[] ObjArr) {
		super();
		this.ObjArr = ObjArr;
	}
	
	public VW_Weight_HESig_ShortArr( LinkedList<VW_Weight_HESig> ObjList) {
		super();
		setArr(ObjList);
	}
	
	/**
	 * 
	 */
	public VW_Weight_HESig_ShortArr() {
		// do nothing
	}

	public void setArr(VW_Weight_HESig[] ObjArr) {
		this.ObjArr = ObjArr;
	}
	
	public void setArr(LinkedList<VW_Weight_HESig> ObjList) {
		this.ObjArr = ObjList.toArray(new VW_Weight_HESig[0]);
	}

	public VW_Weight_HESig[] getArr() {
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
		ObjArr=new VW_Weight_HESig[ArrLength];
		for(int i=0;i<ArrLength;i++){
			ObjArr[i]=new VW_Weight_HESig();
			ObjArr[i].readFields(in);
		}
	}

}
