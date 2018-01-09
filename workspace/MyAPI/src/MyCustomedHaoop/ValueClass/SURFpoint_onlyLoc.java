package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import MyAPI.General.General;
import MyAPI.General.General_geoRank;


/**
 *  x-width, y-height, origin-upperLeftCorner
 */
public class SURFpoint_onlyLoc implements Writable {
	public short x;  
	public short y;
	
	public SURFpoint_onlyLoc(short x, short y) {
		super();
		this.x = x;
		this.y = y;
	}
	
	public SURFpoint_onlyLoc(int[] x_y) {
		this((short)x_y[0], (short)x_y[1]);
	}
	
	public SURFpoint_onlyLoc() {
		super();
		this.x = -1;
		this.y = -1;
	}

	public void setSURFpoint(short x, short y) {
		this.x = x;
		this.y = y;
	}
	
	public short getX() {
		return x;
	}
	
	public short getY() {
		return y;
	}
	
	public String toString() {
		return "x:"+x+", y:"+y;
	}
	
	public boolean isNeighbor_squre(SURFpoint_onlyLoc that, float thr){
		return General_geoRank.isNeighbor_squre(x, y, that.x, that.y, thr);
	}
	
	public boolean isNeighbor_Euclidian(SURFpoint_onlyLoc that, float thr){//to save time, here compare: dist^2 against thr, so thr should be the square verion of the ori thr
		return General.squaredEuclidian(new float[]{x,y},new float[]{that.x,that.y})<thr;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeShort(x);
		out.writeShort(y);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readShort();
		y = in.readShort();
	}

}
