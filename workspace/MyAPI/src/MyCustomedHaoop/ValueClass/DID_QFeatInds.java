package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import MyAPI.Interface.DID;

public class DID_QFeatInds extends IntArr implements DID{

	int docID;
	
	public DID_QFeatInds(int docID, List<Integer> QFeatInds) {
		super(QFeatInds);
		this.docID=docID;
	}
	
	public DID_QFeatInds(){
		super();
	}
	
	@Override
	public int getDID(){
		return docID;
	}
	
	public int[] getQFeatInds(){
		return super.getIntArr();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(docID);
		super.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		docID=in.readInt();
		super.readFields(in);
	}

}
