package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 *  x-width, y-height, origin-upperLeftCorner
 */
public class SURFpoint implements Writable{
	public short x;  
	public short y;
	public float scale;
	public float angle;
		
	public SURFpoint(short x, short y, float scale, float angle) {
		super();
		setSURFpoint(x, y, scale, angle);
	}
	
	public SURFpoint() {
		super();
		this.x = -1;
		this.y = -1;
		this.scale = -1;
		this.angle = -1;
	}
	
	public SURFpoint(SURFpoint point) {
		super();
		setSURFpoint(point);
	}

	public void setSURFpoint(short x, short y, float scale, float angle) {
		this.x = x;
		this.y = y;
		this.scale = scale;
		this.angle = angle;
	}
	
	public void setSURFpoint(SURFpoint point) {
		this.x = point.x;
		this.y = point.y;
		this.scale = point.scale;
		this.angle = point.angle;
	}
	
	public short getX() {
		return x;
	}
	
	public short getY() {
		return y;
	}
	
	public float getScale() {
		return scale;
	}
	
	public float getAngle() {
		return angle;
	}
	
	public SURFpoint getSURFpoint() {
		return new SURFpoint(x,y,scale,angle);
	}
	
	public SURFpoint_onlyLoc getSURFpoint_onlyLoc() {
		return new SURFpoint_onlyLoc(x, y);
	}
	
	public String toString() {
		return "x:"+x+", y:"+y+", scale:"+scale+", angle:"+angle;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeShort(x);
		out.writeShort(y);
		out.writeFloat(scale);
		out.writeFloat(angle);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readShort();
		y = in.readShort();
		scale = in.readFloat();
		angle = in.readFloat();
	}

}
