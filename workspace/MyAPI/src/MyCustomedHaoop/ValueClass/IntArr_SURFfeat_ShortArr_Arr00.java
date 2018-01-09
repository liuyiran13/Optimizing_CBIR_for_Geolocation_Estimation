package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class IntArr_SURFfeat_ShortArr_Arr00 implements Writable{
	//use out.writeShort!! ,so SURFfeat_ShortArr_Arr[i]==SURFfeat_ShortArr its length need to be 0~32767
	IntArr integers;
	SURFfeat_ShortArr_Arr feats;

	public IntArr_SURFfeat_ShortArr_Arr00(int[] integers, SURFfeat[][] ObjArrArr, byte[][] aggSigs) {
		super();
		this.integers = new IntArr(integers);
		this.feats = new SURFfeat_ShortArr_Arr(ObjArrArr,aggSigs);
	}
	
	public IntArr_SURFfeat_ShortArr_Arr00(List<Integer> integers, List<SURFfeat_ShortArr_AggSig> feats) {
		super();
		this.integers = new IntArr(integers);
		this.feats = new SURFfeat_ShortArr_Arr(feats);
	}

	public IntArr_SURFfeat_ShortArr_Arr00() {
		// do nothing
	}
		
	public int[] getIntArr() {
		return integers.getIntArr();
	}
	
	public SURFfeat_ShortArr_AggSig[] getFeats() {
		return feats.getArrArr();
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
		feats=new SURFfeat_ShortArr_Arr();
		feats.readFields(in);
	}
	
}
