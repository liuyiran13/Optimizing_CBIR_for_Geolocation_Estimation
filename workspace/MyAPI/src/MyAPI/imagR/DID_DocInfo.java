package MyAPI.imagR;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import MyCustomedHaoop.ValueClass.DocInfo;

public class DID_DocInfo implements Writable{
	public int DID;
	public DocInfo docInfo;
	
	public DID_DocInfo(int DID, DocInfo docInfo) {
		this.DID=DID;
		this.docInfo=docInfo;
	}
	
	public DID_DocInfo(){
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		DID=in.readInt();
		docInfo=new DocInfo();
		docInfo.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(DID);
		docInfo.write(out);
	}

}
