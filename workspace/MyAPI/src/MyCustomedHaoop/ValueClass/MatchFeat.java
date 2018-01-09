package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MatchFeat implements Writable{
	//use out.writeShort!! ,so SURFfeat_ShortArr_Arr[i]==SURFfeat_ShortArr its length need to be 0~32767
	public short HMDist;
	public short QFeatInd;
	public SURFfeat_noSig docFeat;

	public MatchFeat(short HMDist, short QFeatInd, SURFfeat_noSig docFeat) {
		super();
		this.HMDist = HMDist;
		this.QFeatInd = QFeatInd;
		this.docFeat = docFeat;
	}

	public MatchFeat() {
		// do nothing
	}
	
	public void set(short HMDist, short QFeatInd, SURFfeat_noSig docFeat) {
		this.HMDist = HMDist;
		this.QFeatInd = QFeatInd;
		this.docFeat = docFeat;
	}
	
	public String toString() {
		return "QFeatInd:"+QFeatInd+", HMDist:"+HMDist+", D"+docFeat.toString();
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//write out HMDist
		out.writeShort(HMDist);
		//write out QFeatInd
		out.writeShort(QFeatInd);
		//write out feats
		docFeat.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read HMDist
		HMDist=in.readShort();
		//read QFeatInd
		QFeatInd=in.readShort();
		//read SURFfeat_noSig
		docFeat=new SURFfeat_noSig();
		docFeat.readFields(in);
	}
	
}
