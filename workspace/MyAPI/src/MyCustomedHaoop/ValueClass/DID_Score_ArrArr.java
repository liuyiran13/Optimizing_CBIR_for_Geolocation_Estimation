package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;

import MyAPI.Geo.groupDocs.GroupDocs;
import MyAPI.Interface.DID;
import MyAPI.Interface.Score;

public class DID_Score_ArrArr implements Writable {
	//use out.writeInt!! ,so ObjArr.length need to be 0~2^31-1
	DID_Score_Arr[] ObjArr;

	public DID_Score_ArrArr(DID_Score_Arr[] ObjArr) {
		super();
		this.ObjArr = ObjArr;
	}
	
	public DID_Score_ArrArr(ArrayList<ArrayList<DID_Score>> ObjList){
		super();
		setArr(ObjList);
	}
	
	public <V extends DID & Score, T extends GroupDocs<V>> DID_Score_ArrArr(LinkedList<T> ObjList){
		super();
		this.ObjArr = new DID_Score_Arr[ObjList.size()];
		for (int i = 0; i < ObjList.size(); i++) {
			ObjArr[i]=new DID_Score_Arr(ObjList.get(i).docs);
		}
	}
	
	public DID_Score_ArrArr() {
		// do nothing
	}

	public void setArr(DID_Score_Arr[] ObjArr) {
		this.ObjArr = ObjArr;
	}
	
	public void setArr(ArrayList<ArrayList<DID_Score>> ObjList) {
		this.ObjArr = new DID_Score_Arr[ObjList.size()];
		for (int i = 0; i < ObjList.size(); i++) {
			ObjArr[i]=new DID_Score_Arr(ObjList.get(i).toArray(new DID_Score[0]));
		}
	}

	public DID_Score_Arr[] getArr() {
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
		ObjArr=new DID_Score_Arr[ArrLength];
		for(int i=0;i<ArrLength;i++){
			ObjArr[i]=new DID_Score_Arr();
			ObjArr[i].readFields(in);
		}
	}

}
