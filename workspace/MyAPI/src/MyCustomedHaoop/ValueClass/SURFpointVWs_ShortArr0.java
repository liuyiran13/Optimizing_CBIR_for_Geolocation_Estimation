package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class SURFpointVWs_ShortArr0 implements Writable {
	//use out.writeShort!! ,so ObjArr.length need to be 0~32767
	SURFpointVWs[] ObjArr;

	public SURFpointVWs_ShortArr0( SURFpointVWs[] ObjArr) {
		super();
		this.ObjArr = ObjArr;
	}
	
	public SURFpointVWs_ShortArr0( ArrayList<SURFpointVWs> ObjList) {
		super();
		setArr(ObjList);
	}
	
	public SURFpointVWs_ShortArr0() {
		// do nothing
	}

	public void setArr(SURFpointVWs[] ObjArr) {
		this.ObjArr = ObjArr;
	}
	
	public void setArr(ArrayList<SURFpointVWs> ObjList) {
		this.ObjArr = ObjList.toArray(new SURFpointVWs[0]);
	}

	public SURFpointVWs[] getArr() {
		return ObjArr;
	}

	@Override
	public String toString() {
		return "feat's num: "+ObjArr.length+", assigned vwNum:"+getTotVWNum();
	}
	
	public int getTotVWNum(){
		int vwNum=0;
		for (SURFpointVWs one : ObjArr) {
			vwNum+=one.vws.ObjArr.length;
		}
		return vwNum;
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
		ObjArr=new SURFpointVWs[ArrLength];
		for(int i=0;i<ArrLength;i++){
			ObjArr[i]=new SURFpointVWs();
			ObjArr[i].readFields(in);
		}
	}

}
