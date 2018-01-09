package MyAPI.Obj;

import MyAPI.General.General;

public class DID_VWs {
	public int docID;
	public Integer[] vws;
	
	public DID_VWs(int docID, Integer[] vws) {
		super();
		this.docID = docID;
		this.vws = vws;
	}
	
	public String toString() {
		return "docID:"+docID+", vws:"+General.IntArrToString(vws,"_");
	}
}
