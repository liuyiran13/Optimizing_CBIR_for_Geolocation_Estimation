package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import MyAPI.General.General;
import MyAPI.General.General_IR;

public class TVector implements Writable{

	
	public LinkedList<Int_SURFfeat_ShortArr> docID_feats; 
	
	public TVector() {
		docID_feats=new LinkedList<Int_SURFfeat_ShortArr>();
	}
	
	public void addOneDoc(int photoName, SURFfeat[] featArr, byte[] aggSig){
		docID_feats.add(new Int_SURFfeat_ShortArr(photoName, featArr, aggSig));
	}
	
	public void addOneDoc(int photoName, List<SURFfeat> featArr, byte[] aggSig){
		docID_feats.add(new Int_SURFfeat_ShortArr(photoName, featArr, aggSig));
	}
	
	public int docNum(){
		return docID_feats.size();
	}
	
	public ArrayList<Integer> getDocList(){
		ArrayList<Integer> docs=new ArrayList<>(docID_feats.size()*2);
		for (Int_SURFfeat_ShortArr docFeats : docID_feats) {
			docs.add(docFeats.integer);
		}
		return docs;
	}
	
	public ArrayList<SURFfeat_ShortArr_AggSig> getFeatList(){
		ArrayList<SURFfeat_ShortArr_AggSig> feats=new ArrayList<>(docID_feats.size()*2);
		for (Int_SURFfeat_ShortArr docFeats : docID_feats) {
			feats.add(docFeats.feats);
		}
		return feats;
	}
	
	public void sortByDocID(boolean disp) throws InterruptedException{
		//*** sort based on docID, and out-put to SeqFile ******
		long startTime=System.currentTimeMillis();
		LinkedList<Int_SURFfeat_ShortArr> docFeats_sorted=General_IR.rank_get_AllSortedDocIDs_treeSet(docID_feats, "ASC");
		if(disp==true){
			System.out.println("sort one TVector finished! spend-time:"+General.dispTime(System.currentTimeMillis()-startTime, "s")+", current memory info: "+General.memoryInfo());
			System.out.println("top-10 docs in ASC order:"+docFeats_sorted.subList(0, Math.min(10, docFeats_sorted.size())));
		}
		docID_feats=docFeats_sorted;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size=in.readInt();
		docID_feats=new LinkedList<>();
		if (size>0) {//exist feats
			docID_feats=new LinkedList<>();
			for (int i = 0; i < size; i++) {
				Int_SURFfeat_ShortArr temp=new Int_SURFfeat_ShortArr();
				temp.readFields(in);
				docID_feats.add(temp);
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(docID_feats.size());
		if (docID_feats.size()>0) {//exist feats
			for (Int_SURFfeat_ShortArr one : docID_feats) {
				one.write(out);
			}
		}
	}

}
