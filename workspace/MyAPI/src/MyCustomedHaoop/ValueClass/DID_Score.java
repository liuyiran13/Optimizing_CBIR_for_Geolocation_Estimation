package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import MyAPI.Interface.DID;
import MyAPI.Interface.Score;


public class DID_Score implements Writable,DID,Score{
	public int docID;
	public float score;
	
	public DID_Score(int docID, float score) {
		super();
		this.docID = docID;
		this.score = score;
	}
	
	public DID_Score() {
		super();
		this.docID = -1;
		this.score = -1;
	}

	public String toString() {
		return "docID:"+docID+", score:"+score;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(docID);
		out.writeFloat(score);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		docID = in.readInt();
		score = in.readFloat();
	}

	@Override
	public int getDID() {
		return docID;
	}

	@Override
	public float getScore() {
		return score;
	}

	public static <V extends DID & Score> DID_Score get_DIDScore(V one){
		return new DID_Score(one.getDID(), one.getScore());
	}
	
	public static <V extends DID & Score> LinkedList<DID_Score> get_DIDScoreList(List<V> input){
		LinkedList<DID_Score> res=new LinkedList<>();
		for (V one : input) {
			res.add(DID_Score.get_DIDScore(one));
		}
		return res;
	}
}
