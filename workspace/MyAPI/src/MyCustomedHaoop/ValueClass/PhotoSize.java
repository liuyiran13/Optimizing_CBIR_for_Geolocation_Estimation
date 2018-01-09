package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PhotoSize implements Writable{

	public int w;
	public int h;
	
	public PhotoSize(int w, int h) {
		this.w=w;
		this.h=h;
	}
	
	public PhotoSize() {
		// do nothing, you must have one no parameter constructor for Writable, so hadoop can creat a empty one and read from bytes
	}
		
	@Override
	public String toString(){
		return "w:"+w+", h:"+h;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		w=in.readInt();
		h=in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(w);
		out.writeInt(h);
	}

}
