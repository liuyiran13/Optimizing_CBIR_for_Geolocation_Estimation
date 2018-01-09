package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

/**
 *  for hadoop key, it must implements WritableComparable, not just implements Writable for value!!
 *
 */
public class DouArr implements Writable {
	double[] DouArr;

	public DouArr( double[] DouArr) {
		super();
		this.DouArr = DouArr;
	}
	
	public DouArr( ArrayList<Double> List) {
		super();
		setDouArr(List);
	}
	
	/**
	 * 
	 */
	public DouArr() {
		// do nothing
	}

	public void setDouArr(double[] DouArr) {
		this.DouArr = DouArr;
	}
	
	public void setDouArr(ArrayList<Double> List) {
		this.DouArr=new double[List.size()];
		//make integers
		for(int i=0;i<List.size();i++)
			this.DouArr[i]=List.get(i);
	}

	public double[] getDouArr() {
		return DouArr;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(DouArr.length);
		for(int i=0;i<DouArr.length;i++){
			out.writeDouble(DouArr[i]);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int DouArrLength=in.readInt();
		DouArr=new double[DouArrLength];
		for(int i=0;i<DouArrLength;i++){
			DouArr[i]=in.readDouble();
		}
	}

}
