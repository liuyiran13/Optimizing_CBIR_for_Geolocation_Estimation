package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class DocAllMatchFeats implements Writable{
	public int DocID;
	public Int_MatchFeatArr_Arr feats;

	public DocAllMatchFeats(int DocID, Int_MatchFeatArr[] feats) {
		super();
		this.DocID = DocID;
		this.feats = new Int_MatchFeatArr_Arr(feats);
	}
	
	public DocAllMatchFeats(int DocID, Int_MatchFeatArr_Arr feats) {
		super();
		this.DocID = DocID;
		this.feats = feats;
	}

	public DocAllMatchFeats() {
		// do nothing
	}
	
	public void set(int DocID, Int_MatchFeatArr[] feats) {
		this.DocID = DocID;
		this.feats = new Int_MatchFeatArr_Arr(feats);
	}
	
	public int getMatchNum(){
		int matchNum=0;
		for (Int_MatchFeatArr one : feats.feats) {
			matchNum+=one.feats.ObjArr.length;
		}
		return matchNum;
	}
	
	public ArrayList<MatchFeat_VW> getMatchFeat_VW(){
		ArrayList<MatchFeat_VW> res=new ArrayList<MatchFeat_VW>(getMatchNum()*2);
		for (Int_MatchFeatArr oneVW : feats.feats) {
			for (MatchFeat one : oneVW.feats.ObjArr) {
				res.add(new MatchFeat_VW(one, oneVW.Integer));
			}
		}
		return res;
	}
	
	public String toString(){
		return "docID_"+DocID+", matchVWNum_"+feats.feats.length+", matchFeatNum_"+getMatchNum();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//write out Integer
		out.writeInt(DocID);
		//write out Int_MatchFeatArr_Arr
		feats.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read Integer
		DocID=in.readInt();
		//read Int_MatchFeatArr_Arr
		feats=new Int_MatchFeatArr_Arr();
		feats.readFields(in);
	}
	
}
