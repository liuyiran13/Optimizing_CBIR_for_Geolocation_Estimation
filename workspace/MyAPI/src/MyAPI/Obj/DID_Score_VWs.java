package MyAPI.Obj;

import java.util.ArrayList;

import MyCustomedHaoop.ValueClass.DID_Score;

public class DID_Score_VWs {
	public int docID;
	public float score;
	public ArrayList<Integer> vws;
	
	public DID_Score_VWs(int docID, float score, ArrayList<Integer> vws) {
		super();
		this.docID = docID;
		this.score = score;
		this.vws = vws;
	}
	
	public DID_Score_VWs(DID_Score doc_score, ArrayList<Integer> vws) {
		super();
		this.docID = doc_score.docID;
		this.score = doc_score.score;
		this.vws = vws;
	}
	
	public String toString() {
		return "docID:"+docID+", score:"+score+", vws:"+vws;
	}
}
