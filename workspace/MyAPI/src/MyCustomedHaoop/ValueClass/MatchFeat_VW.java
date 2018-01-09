package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatchFeat_VW extends MatchFeat{
	public int vw;

	public MatchFeat_VW(short HMDist, short QFeatInd, SURFfeat_noSig docFeat, int vw) {
		super(HMDist, QFeatInd, docFeat);
		this.vw = vw;
	}
	
	public MatchFeat_VW(MatchFeat matchFeat,int vw) {
		super(matchFeat.HMDist, matchFeat.QFeatInd, matchFeat.docFeat);
		this.vw = vw;
	}

	public MatchFeat_VW() {
		// do nothing
	}
	
	public String toString() {
		return "vw:"+vw+", QFeatInd:"+QFeatInd+", HMDist:"+HMDist+", D"+docFeat.toString();
	}
	
	public void set(int vw, short HMDist, short QFeatInd, SURFfeat_noSig docFeat) {
		this.HMDist = HMDist;
		this.QFeatInd = QFeatInd;
		this.docFeat = docFeat;
		this.vw = vw;
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
		//write out vw
		out.writeInt(vw);
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
		//read vw
		vw=in.readInt();
	}
	
}
