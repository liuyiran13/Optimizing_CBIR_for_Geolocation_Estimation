package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

import MyAPI.Obj.SelectID.IDForSelection;


public class Int_SURFfeat_ShortArr implements Writable, IDForSelection, Comparable<Int_SURFfeat_ShortArr>{
	public int integer; //docID or VW
	public SURFfeat_ShortArr_AggSig feats;
	
	public Int_SURFfeat_ShortArr(int integer, SURFfeat[] feats, byte[] aggSig) {
		super();
		this.integer = integer;
		this.feats = new SURFfeat_ShortArr_AggSig(feats, aggSig);
	}
	
	public Int_SURFfeat_ShortArr(int integer, List<SURFfeat> feats, byte[] aggSig) {
		super();
		set(integer, feats, aggSig);
	}
	
	/**
	 * 
	 */
	public Int_SURFfeat_ShortArr() {
		// do nothing
	}
	
	public void set(int integer, List<SURFfeat> feats, byte[] aggSig) {
		this.integer = integer;
		this.feats = new SURFfeat_ShortArr_AggSig(feats, aggSig);
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//write out Int
		out.writeInt(integer);
		//write out feats
		feats.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read Int
		integer=in.readInt();
		//read SURFfeat_List
		feats=new SURFfeat_ShortArr_AggSig();
		feats.readFields(in);
	}

	@Override
	public int getIDForSelection() {
		return integer;
	}
	
	@Override
	public String toString(){
		return "integer:"+integer+", feats:"+feats;
	}

	@Override
	public int compareTo(Int_SURFfeat_ShortArr that) {
		int compareValue = (int) Math.signum(this.integer - that.integer);
        if (compareValue==0) {//for remove method, it need exactly obj match! so same obj, same address, same hashcode!
        	compareValue=this.hashCode()-that.hashCode();
        }
        return compareValue;
	}
	
}
