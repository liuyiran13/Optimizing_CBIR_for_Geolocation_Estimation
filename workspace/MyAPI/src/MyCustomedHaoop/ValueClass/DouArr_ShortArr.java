package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class DouArr_ShortArr implements Writable{
	DouArr[] ObjArrArr;

	public DouArr_ShortArr( double[][] ObjArrArr) {
		super();
		this.ObjArrArr = new DouArr[ObjArrArr.length];
		for (int i = 0; i < ObjArrArr.length; i++) {
			this.ObjArrArr[i]=new DouArr(ObjArrArr[i]);
		}
	}
	
	public DouArr_ShortArr( ArrayList<double[]> ObjArrList) {
		super();
		setArrArr(ObjArrList);
	}
	
	public DouArr_ShortArr() {
		// do nothing
	}

	public void setArrArr(double[][] ObjArrArr) {
		this.ObjArrArr = new DouArr[ObjArrArr.length];
		for (int i = 0; i < ObjArrArr.length; i++) {
			this.ObjArrArr[i]=new DouArr(ObjArrArr[i]);
		}
	}
	
	public void setArrArr(ArrayList<double[]> ObjArrList) {
		ObjArrArr = new DouArr[ObjArrList.size()];
		for (int i = 0; i < ObjArrList.size(); i++) {
			ObjArrArr[i]=new DouArr(ObjArrList.get(i));
		}
	}

	public DouArr[] getArrArr() {
		return ObjArrArr;
	}
	
	public double[][] getRawArrArr() {
		double[][] rawArrArr=new double[ObjArrArr.length][];
		for (int i = 0; i < rawArrArr.length; i++) {
			rawArrArr[i]=ObjArrArr[i].getDouArr();
		}
		return rawArrArr;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeShort(ObjArrArr.length);
		for(int i=0;i<ObjArrArr.length;i++){
			ObjArrArr[i].write(out);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int ArrArrLength=in.readShort();
		ObjArrArr=new DouArr[ArrArrLength];
		for(int i=0;i<ArrArrLength;i++){
			ObjArrArr[i]=new DouArr();
			ObjArrArr[i].readFields(in);
		}
	}
	

}
