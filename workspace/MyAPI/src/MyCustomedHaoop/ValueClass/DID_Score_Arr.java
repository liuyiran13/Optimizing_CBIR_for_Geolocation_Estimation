package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import MyAPI.Interface.DID;
import MyAPI.Interface.Score;

public class DID_Score_Arr implements Writable {
	//use out.writeInt!! ,so ObjArr.length need to be 0~2^31-1
	DID_Score[] ObjArr;

	public DID_Score_Arr(DID_Score[] ObjArr) {
		super();
		this.ObjArr = ObjArr;
	}
	
	public DID_Score_Arr(ArrayList<DID_Score> ObjList){
		super();
		setArr(ObjList);
	}
	
	public <V extends DID & Score> DID_Score_Arr(List<V> ObjList){
		super();
		setArr(DID_Score.get_DIDScoreList(ObjList).toArray(new DID_Score[0]));
	}

	public DID_Score_Arr() {
		// do nothing
	}

	public void setArr(DID_Score[] ObjArr) {
		this.ObjArr = ObjArr;
	}
	
	public void setArr(ArrayList<DID_Score> ObjList) {
		this.ObjArr = ObjList.toArray(new DID_Score[0]);
	}

	public DID_Score[] getArr() {
		return ObjArr;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(ObjArr.length);
		for(int i=0;i<ObjArr.length;i++){
			ObjArr[i].write(out);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int ArrLength=in.readInt();
		ObjArr=new DID_Score[ArrLength];
		for(int i=0;i<ArrLength;i++){
			ObjArr[i]=new DID_Score();
			ObjArr[i].readFields(in);
		}
	}

}
