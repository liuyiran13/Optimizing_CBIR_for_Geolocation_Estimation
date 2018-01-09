package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


public class DocInfo implements Writable {
	public short pointNum;
	public float BoVWVectorNorm;
	public float IDFBoVWVectorNorm;
	public float IDF1VW1FeatVectorNorm;//used for ASMK norm
	public short width;
	public short height;
	
	public DocInfo(short pointNum, float BoVWVectorNorm, float IDFBoVWVectorNorm, float IDF1VW1FeatVectorNorm, short width, short height) {
		super();
		this.pointNum = pointNum;
		this.BoVWVectorNorm = BoVWVectorNorm;
		this.IDFBoVWVectorNorm = IDFBoVWVectorNorm;
		this.IDF1VW1FeatVectorNorm = IDF1VW1FeatVectorNorm;
		this.width = width;
		this.height = height;
	}
	
	public DocInfo(DocInfo docInfo) {
		super();
		this.pointNum = docInfo.pointNum;
		this.BoVWVectorNorm = docInfo.BoVWVectorNorm;
		this.IDFBoVWVectorNorm = docInfo.IDFBoVWVectorNorm;
		this.IDF1VW1FeatVectorNorm = docInfo.IDF1VW1FeatVectorNorm;
		this.width = docInfo.width;
		this.height = docInfo.height;
	}
	
	public DocInfo() {
		super();
		this.pointNum = -1;
		this.BoVWVectorNorm = -1;
		this.IDFBoVWVectorNorm = -1;
		this.IDF1VW1FeatVectorNorm = -1;
		this.width = -1;
		this.height = -1;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeShort(pointNum);
		out.writeFloat(BoVWVectorNorm);
		out.writeFloat(IDFBoVWVectorNorm);
		out.writeFloat(IDF1VW1FeatVectorNorm);
		out.writeShort(width);
		out.writeShort(height);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		pointNum = in.readShort();
		BoVWVectorNorm = in.readFloat();
		IDFBoVWVectorNorm = in.readFloat();
		IDF1VW1FeatVectorNorm = in.readFloat();
		width = in.readShort();
		height = in.readShort();
	}

}
