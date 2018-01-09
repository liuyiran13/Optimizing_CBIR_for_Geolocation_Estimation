package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import MyAPI.General.General;

/**
 *	general writable class for saving BytesValue
 *
 */
public class BytesValue implements Writable{
	protected byte[] byteContent;
	public boolean isBytesValue;
	public boolean isBufferedImage_jpg;
	public boolean isVideoBytes;
		
	public BytesValue(byte[] byteContent) {
		super();
		this.byteContent = byteContent;
	}
	
	public BytesValue(File file, boolean disp, String spacer) throws InterruptedException {
		super();
		this.byteContent = General.readBinaryFile(file, disp, spacer);
	}
	
	public BytesValue(String filePath, boolean disp, String spacer) throws InterruptedException {
		super();
		this.byteContent = General.readBinaryFile(new File(filePath), disp, spacer);
	}
	
	//for generic class object
	public BytesValue(String targetClass) throws InterruptedException {
		super();
		this.isBytesValue = false; this.isBufferedImage_jpg = false; this.isVideoBytes = false;
		if (targetClass.equalsIgnoreCase("BytesValue")) {
			this.isBytesValue = true;
		}else if (targetClass.equalsIgnoreCase("BufferedImage_jpg")) {
			this.isBufferedImage_jpg = true;
		}else if (targetClass.equalsIgnoreCase("VideoBytes")) {
			this.isVideoBytes = true;
		}else {
			throw new InterruptedException("err in BytesValue, targetClass should be BytesValue, BufferedImage_jpg or VideoBytes, here:"+targetClass);
		}
	}
	
	//for generic class name
	public static Class<? extends BytesValue> getTargetClass(String targetClass) throws InterruptedException {
		if (targetClass.equalsIgnoreCase("BytesValue")) {
			return BytesValue.class;
		}else if (targetClass.equalsIgnoreCase("BufferedImage_jpg")) {
			return BufferedImage_jpg.class;
		}else if (targetClass.equalsIgnoreCase("VideoBytes")) {
			return VideoBytes.class;
		}else {
			throw new InterruptedException("err in BytesValue, targetClass should be BytesValue, BufferedImage_jpg or VideoBytes, here:"+targetClass);
		}
	}
	
	public BytesValue() {
		// do nothing
	}
	
	public Object getTargetClassObj(byte[] byteContent) throws InterruptedException {
		if (isBytesValue) {
			return new BytesValue(byteContent);
		}else if (isBufferedImage_jpg) {
			return new BufferedImage_jpg(byteContent);
		}else if (isVideoBytes) {
			return new VideoBytes(byteContent);
		}else {
			throw new InterruptedException("err in returnTargetClass, please initialise BytesValue with correct targetClass");
		}
	}
	
	public void setBytesValue(byte[] byteContent){
		this.byteContent = byteContent;
	}
	
	public void setBytesValue(File file, boolean disp, String spacer) throws InterruptedException {
		this.byteContent = General.readBinaryFile(file, disp, spacer);
	}

	public void setBytesValue(String filePath, boolean disp, String spacer) throws InterruptedException {
		this.byteContent = General.readBinaryFile(new File(filePath), disp, spacer);
	}
	
	public byte[] getBytes(){
		return this.byteContent;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//write out byte[]
		out.writeInt(byteContent.length);
		out.write(byteContent);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		byteContent = new byte[in.readInt()];
		in.readFully(byteContent);
	}
}
