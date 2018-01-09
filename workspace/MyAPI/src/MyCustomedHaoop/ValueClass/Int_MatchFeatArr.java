package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import MyCustomedHaoop.ValueClass.ArrWritableClassCollection.MatchFeat_Arr;

public class Int_MatchFeatArr implements Writable{
	//use out.writeShort!! ,so MatchFeat_ShortArr_Arr[i]==MatchFeat_ShortArr its length need to be 0~32767
	public int Integer;
	public MatchFeat_Arr feats;

	public Int_MatchFeatArr(int Integer, MatchFeat_Arr feats) {
		super();
		this.Integer = Integer;
		this.feats = feats;
	}

	public Int_MatchFeatArr() {
		// do nothing
	}
	
	public void set(int Integer, MatchFeat_Arr feats) {
		this.Integer = Integer;
		this.feats = feats;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//write out Integer
		out.writeInt(Integer);
		//write out MatchFeat_ShortArr
		feats.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read Integer
		Integer=in.readInt();
		//read SURFfeat_ShortArr_Arr
		feats=new MatchFeat_Arr();
		feats.readFields(in);
	}
	
}
