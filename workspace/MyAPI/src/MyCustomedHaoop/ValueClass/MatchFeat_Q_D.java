package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MatchFeat_Q_D implements Writable{
	//use out.writeShort!! ,so SURFfeat_ShortArr_Arr[i]==SURFfeat_ShortArr its length need to be 0~32767
	public short HMDist;
	public SURFfeat_noSig queryFeat;
	public SURFfeat_noSig docFeat;

	public MatchFeat_Q_D(short HMDist, SURFfeat_noSig queryFeat, SURFfeat_noSig docFeat) {
		super();
		this.HMDist = HMDist;
		this.queryFeat = queryFeat;
		this.docFeat = docFeat;
	}

	public MatchFeat_Q_D() {
		// do nothing
	}
	
	public void set(short HMDist, SURFfeat_noSig queryFeat, SURFfeat_noSig docFeat) {
		this.HMDist = HMDist;
		this.queryFeat = queryFeat;
		this.docFeat = docFeat;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//write out HMDist
		out.writeShort(HMDist);
		//write out queryFeat
		queryFeat.write(out);
		//write out docFeat
		docFeat.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read HMDist
		HMDist=in.readShort();
		//read queryFeat
		queryFeat=new SURFfeat_noSig();
		queryFeat.readFields(in);
		//read docFeat
		docFeat=new SURFfeat_noSig();
		docFeat.readFields(in);
	}
	
}
