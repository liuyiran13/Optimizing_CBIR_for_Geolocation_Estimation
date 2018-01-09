package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class IntArrArr implements Writable{
	IntArr[] ObjArrArr;

	public IntArrArr( int[][] ObjArrArr) {
		super();
		this.ObjArrArr = new IntArr[ObjArrArr.length];
		for (int i = 0; i < ObjArrArr.length; i++) {
			this.ObjArrArr[i]=new IntArr(ObjArrArr[i]);
		}
	}
	
	public IntArrArr( List<int[]> ObjArrList) {
		super();
		setArrArr(ObjArrList);
	}
	
	public IntArrArr() {
		// do nothing
	}

	public void setArrArr(int[][] ObjArrArr) {
		this.ObjArrArr = new IntArr[ObjArrArr.length];
		for (int i = 0; i < ObjArrArr.length; i++) {
			this.ObjArrArr[i]=new IntArr(ObjArrArr[i]);
		}
	}
	
	public void setArrArr(List<int[]> ObjArrList) {
		ObjArrArr = new IntArr[ObjArrList.size()];
		int i=0;
		for (int[] one:ObjArrList) {
			ObjArrArr[i]=new IntArr(one);
			i++;
		}
	}

	public IntArr[] getArrArr() {
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
		ObjArrArr=new IntArr[ArrArrLength];
		for(int i=0;i<ArrArrLength;i++){
			ObjArrArr[i]=new IntArr();
			ObjArrArr[i].readFields(in);
		}
	}
	

}
