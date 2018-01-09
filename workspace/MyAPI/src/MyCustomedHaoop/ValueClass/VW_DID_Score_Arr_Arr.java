package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class VW_DID_Score_Arr_Arr implements Writable {
	//use out.writeInt!! ,so ObjArr.length need to be 0~2^31-1
	VW_DID_Score_Arr[] ObjArr;

	public VW_DID_Score_Arr_Arr(VW_DID_Score_Arr[] ObjArr) {
		super();
		this.ObjArr = ObjArr;
	}
	
	public VW_DID_Score_Arr_Arr(ArrayList<VW_DID_Score_Arr> ObjList){
		super();
		setArr(ObjList);
	}

	public VW_DID_Score_Arr_Arr() {
		// do nothing
	}

	public void setArr(VW_DID_Score_Arr[] ObjArr) {
		this.ObjArr = ObjArr;
	}
	
	public void setArr(ArrayList<VW_DID_Score_Arr> ObjList) {
		this.ObjArr = ObjList.toArray(new VW_DID_Score_Arr[0]);
	}

	public VW_DID_Score_Arr[] getArr() {
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
		ObjArr=new VW_DID_Score_Arr[ArrLength];
		for(int i=0;i<ArrLength;i++){
			ObjArr[i]=new VW_DID_Score_Arr();
			ObjArr[i].readFields(in);
		}
	}

}
