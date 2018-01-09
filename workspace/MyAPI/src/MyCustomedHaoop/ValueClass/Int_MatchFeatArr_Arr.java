package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class Int_MatchFeatArr_Arr implements Writable{
	public Int_MatchFeatArr[] feats;

	public Int_MatchFeatArr_Arr(Int_MatchFeatArr[] feats) {
		super();
		this.feats = feats;
	}
	
	public Int_MatchFeatArr_Arr(List<Int_MatchFeatArr> feats) {
		super();
		this.feats = feats.toArray(new Int_MatchFeatArr[0]);
	}

	public Int_MatchFeatArr_Arr() {
		// do nothing
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//write out Int_MatchFeatArr[]
		int featLength=feats.length;
		out.writeInt(featLength);
		for (int i = 0; i < featLength; i++) {
			feats[i].write(out);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read Int_MatchFeatArr[]
		int featLength=in.readInt();
		feats=new Int_MatchFeatArr[featLength];
		for (int i = 0; i < featLength; i++) {
			feats[i]= new Int_MatchFeatArr();
			feats[i].readFields(in);
		}
		
	}
	
}
