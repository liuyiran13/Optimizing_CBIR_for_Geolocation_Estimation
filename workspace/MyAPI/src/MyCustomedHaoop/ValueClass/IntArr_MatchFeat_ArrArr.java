package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class IntArr_MatchFeat_ArrArr implements Writable{
	//use out.writeShort!! ,so MatchFeat_ShortArr_Arr[i]==MatchFeat_ShortArr its length need to be 0~32767
	IntArr integers;
	MatchFeat_ArrArr feats;

	public IntArr_MatchFeat_ArrArr(IntArr integers, MatchFeat_ArrArr feats) {
		super();
		this.integers = integers;
		this.feats = feats;
	}

	public IntArr_MatchFeat_ArrArr() {
		// do nothing
	}
	
	public void set(IntArr integers, MatchFeat_ArrArr feats) {
		this.integers = integers;
		this.feats = feats;
	}
	
	public IntArr getIntegers() {
		return integers;
	}
	
	public MatchFeat_ArrArr getFeats() {
		return feats;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//write out IntArr
		integers.write(out);
		//write out SURFfeat_ShortArr_Arr
		feats.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read IntArr
		integers=new IntArr();
		integers.readFields(in);
		//read SURFfeat_ShortArr_Arr
		feats=new MatchFeat_ArrArr();
		feats.readFields(in);
	}
	
}
