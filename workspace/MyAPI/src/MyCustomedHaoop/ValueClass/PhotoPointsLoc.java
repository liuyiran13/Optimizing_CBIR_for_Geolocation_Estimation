package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

import MyCustomedHaoop.ValueClass.ArrWritableClassCollection.SURFPointOnlyLoc_Arr;

public class PhotoPointsLoc implements Writable{
	public int width;
	public int height;
	public SURFPointOnlyLoc_Arr feats;
	
	public PhotoPointsLoc(int width, int height, SURFPointOnlyLoc_Arr feats) {
		super();
		this.width = width;
		this.height = height;
		this.feats = feats;
	}
	
	public PhotoPointsLoc(int width, int height, SURFpoint_onlyLoc[] feats) {
		super();
		this.width = width;
		this.height = height;
		this.feats = new SURFPointOnlyLoc_Arr(feats);
	}

	public PhotoPointsLoc() {
		// do nothing
	}
	
	public void set(int width, int height, SURFPointOnlyLoc_Arr feats) {
		this.width = width;
		this.height = height;
		this.feats = feats;
	}
	
	@Override
	public String toString() {
		return "width:"+width+", height:"+height+", feats:"+feats;
	}

	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//write out width and height
		out.writeInt(width);
		out.writeInt(height);
		//write out DID_Score_Arr
		feats.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read width
		width=in.readInt();
		height=in.readInt();
		//read SURFPointOnlyLoc_Arr
		feats=new SURFPointOnlyLoc_Arr();
		feats.readFields(in);
	}
	
}
