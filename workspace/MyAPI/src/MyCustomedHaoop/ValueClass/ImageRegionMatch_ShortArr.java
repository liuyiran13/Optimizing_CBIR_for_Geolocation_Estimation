package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class ImageRegionMatch_ShortArr implements Writable {
	//use out.writeShort!! ,so ObjArr.length need to be 0~32767
	public ImageRegionMatch[] ObjArr;

	public ImageRegionMatch_ShortArr( ImageRegionMatch[] ObjArr) {
		super();
		this.ObjArr = ObjArr;
	}
	
	public ImageRegionMatch_ShortArr( List<ImageRegionMatch> ObjList) {
		super();
		setArr(ObjList);
	}
	
	/**
	 * 
	 */
	public ImageRegionMatch_ShortArr() {
		// do nothing
	}

	public void setArr(ImageRegionMatch[] ObjArr) {
		this.ObjArr = ObjArr;
	}
	
	public void setArr(List<ImageRegionMatch> ObjList) {
		this.ObjArr = ObjList.toArray(new ImageRegionMatch[0]);
	}
	
	public List<ImageRegionMatch> getList(){
		return Arrays.asList(ObjArr);
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
		ObjArr=new ImageRegionMatch[ArrLength];
		for(int i=0;i<ArrLength;i++){
			ObjArr[i]=new ImageRegionMatch();
			ObjArr[i].readFields(in);
		}
	}

}
