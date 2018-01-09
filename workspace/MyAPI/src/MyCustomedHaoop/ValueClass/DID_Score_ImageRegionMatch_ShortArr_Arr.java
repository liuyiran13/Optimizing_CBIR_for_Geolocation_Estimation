package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class DID_Score_ImageRegionMatch_ShortArr_Arr implements Writable {
	public DID_Score_ImageRegionMatch_ShortArr[] ObjArr;

	public DID_Score_ImageRegionMatch_ShortArr_Arr( DID_Score_ImageRegionMatch_ShortArr[] ObjArr) {
		super();
		this.ObjArr = ObjArr;
	}
	
	public DID_Score_ImageRegionMatch_ShortArr_Arr( List<DID_Score_ImageRegionMatch_ShortArr> ObjList) {
		super();
		setArr(ObjList);
	}
	
	/**
	 * 
	 */
	public DID_Score_ImageRegionMatch_ShortArr_Arr() {
		// do nothing
	}

	public void setArr(DID_Score_ImageRegionMatch_ShortArr[] ObjArr) {
		this.ObjArr = ObjArr;
	}
	
	public void setArr(List<DID_Score_ImageRegionMatch_ShortArr> ObjList) {
		this.ObjArr = ObjList.toArray(new DID_Score_ImageRegionMatch_ShortArr[0]);
	}
	
	public static ArrayList<Integer> extractDIDs(List<DID_Score_ImageRegionMatch_ShortArr> list){
		ArrayList<Integer> DIDs=new ArrayList<Integer>();
		for (DID_Score_ImageRegionMatch_ShortArr one : list) {
			DIDs.add(one.getDID());
		}
		return DIDs;
	}
	
	public static ArrayList<Float> extractScores(List<DID_Score_ImageRegionMatch_ShortArr> list){
		ArrayList<Float> Scores=new ArrayList<Float>();
		for (DID_Score_ImageRegionMatch_ShortArr one : list) {
			Scores.add(one.getScore());
		}
		return Scores;
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
		ObjArr=new DID_Score_ImageRegionMatch_ShortArr[ArrLength];
		for(int i=0;i<ArrLength;i++){
			ObjArr[i]=new DID_Score_ImageRegionMatch_ShortArr();
			ObjArr[i].readFields(in);
		}
	}

}
