package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;

public class SURFpoint_ShortArr implements Writable {
	//use out.writeShort!! ,so ObjArr.length need to be 0~32767
	SURFpoint[] ObjArr;

	public SURFpoint_ShortArr( SURFpoint[] ObjArr) {
		super();
		this.ObjArr = ObjArr;
	}
	
	public SURFpoint_ShortArr( ArrayList<SURFpoint> ObjList) {
		super();
		setArr(ObjList);
	}
	
	public SURFpoint_ShortArr() {
		// do nothing, you must have one no parameter constructor for Writable, so hadoop can creat a empty one and read from bytes
	}

	public void setArr(SURFpoint[] ObjArr) {
		this.ObjArr = ObjArr;
	}
	
	public void setArr(ArrayList<SURFpoint> ObjList) {
		this.ObjArr = ObjList.toArray(new SURFpoint[0]);
	}

	public SURFpoint[] getArr() {
		return ObjArr;
	}
	
	public SURFpoint_onlyLoc[] getSURFPointOnlyLoc(){
		LinkedList<SURFpoint_onlyLoc> res=new LinkedList<>();
		for (SURFpoint one : ObjArr) {
			res.add(one.getSURFpoint_onlyLoc());
		}
		return res.toArray(new SURFpoint_onlyLoc[0]);
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
		ObjArr=new SURFpoint[ArrLength];
		for(int i=0;i<ArrLength;i++){
			ObjArr[i]=new SURFpoint();
			ObjArr[i].readFields(in);
		}
	}

}
