package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

import MyCustomedHaoop.ValueClass.ArrWritableClassCollection.MatchFeat_Arr;

public class MatchFeat_ArrArr implements Writable {
	//use MatchFeat_ShortArr!! ,so ObjArrArr[i]=MatchFeat_ShortArr, its length need to be 0~32767
	MatchFeat_Arr[] ObjArrArr;

	public MatchFeat_ArrArr( MatchFeat_Arr[] ObjArrArr) {
		super();
		this.ObjArrArr = ObjArrArr;
	}
	
	public MatchFeat_ArrArr( ArrayList<MatchFeat_Arr> ObjArrList) {
		super();
		setArrArr(ObjArrList);
	}
	
	/**
	 * 
	 */
	public MatchFeat_ArrArr() {
		// do nothing
	}
	
	public void setArrArr(ArrayList<MatchFeat_Arr> ObjArrList) {
		ObjArrArr = ObjArrList.toArray(new MatchFeat_Arr[0]);
	}

	public MatchFeat_Arr[] getArrArr() {
		return ObjArrArr;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(ObjArrArr.length);
		for(int i=0;i<ObjArrArr.length;i++){
			ObjArrArr[i].write(out);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int ArrArrLength=in.readInt();
		ObjArrArr=new MatchFeat_Arr[ArrArrLength];
		for(int i=0;i<ArrArrLength;i++){
			ObjArrArr[i]=new MatchFeat_Arr();
			ObjArrArr[i].readFields(in);
		}
	}

}
