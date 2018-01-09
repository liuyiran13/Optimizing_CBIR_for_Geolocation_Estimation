package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

import MyAPI.Interface.DID;
import MyAPI.Interface.Score;

public class DID_Score_ImageRegionMatch_ShortArr implements Writable,DID,Score{
	//use out.writeShort!! ,so HESig_ShortArr_Arr[i]==HESig_ShortArr its length need to be 0~32767
	public DID_Score dID_score;
	public ImageRegionMatch_ShortArr matches;

	public DID_Score_ImageRegionMatch_ShortArr(int dID, float score, ImageRegionMatch[] matches) {
		super();
		this.dID_score = new DID_Score(dID,score);
		this.matches = new ImageRegionMatch_ShortArr(matches);
	}
	
	public DID_Score_ImageRegionMatch_ShortArr(int dID, float score, List<ImageRegionMatch> matches) {
		super();
		this.dID_score = new DID_Score(dID,score);
		this.matches = new ImageRegionMatch_ShortArr(matches);
	}

	public DID_Score_ImageRegionMatch_ShortArr() {
		// do nothing
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//write out IntArr
		dID_score.write(out);
		//write out HESig_ShortArr_Arr
		matches.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read IntArr
		dID_score=new DID_Score();
		dID_score.readFields(in);
		//read HESig_ShortArr_Arr
		matches=new ImageRegionMatch_ShortArr();
		matches.readFields(in);
	}

	@Override
	public int getDID() {
		return dID_score.docID;
	}

	@Override
	public float getScore() {
		// TODO Auto-generated method stub
		return dID_score.score;
	}
	
	public List<ImageRegionMatch> getMatches(){
		return matches.getList();
	}
	
}
