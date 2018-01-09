package MyAPI.imagR;

import java.util.ArrayList;

import MyAPI.General.General_IR;
import MyAPI.Obj.Disp;

public class MakeRank <T extends Object>{
	
	int topRank;
	boolean isConcateTwoList;
	ArrayList<T> docIDs_0; ArrayList<Float> docScores_0;//main rank
	ArrayList<T> docIDs_1; ArrayList<Float> docScores_1;//second rank
	
	public MakeRank(Disp disp, int topRank, boolean isConcateTwoList, int estMaxRankLength){
		this.topRank=topRank;
		this.isConcateTwoList=isConcateTwoList;
		docIDs_0=new ArrayList<T>(estMaxRankLength); docScores_0=new ArrayList<Float>(estMaxRankLength);
		docIDs_1=new ArrayList<T>(estMaxRankLength); docScores_1=new ArrayList<Float>(estMaxRankLength);
		disp.disp("setup_makeRank finished! topRank: "+topRank+", isConcateTwoList:"+isConcateTwoList);
	}
	
	public void addOneDoc(T doc, float[] scores){
		if (scores[0]!=0) {
			docScores_0.add(scores[0]);
			docIDs_0.add(doc);
		}else {
			docScores_1.add(scores[1]);
			docIDs_1.add(doc);
		}
	}
	
	public void addOneDoc_onlyMainRank(T doc, float score){
		docScores_0.add(score);
		docIDs_0.add(doc);
	}
	
	public int getScoredDocNum(int whichList) throws InterruptedException{
		if(whichList==-1){
			return docIDs_0.size()+docIDs_1.size();
		}else if (whichList==0) {
			return docIDs_0.size();
		}else if (whichList==1) {
			return docIDs_1.size();
		}else{
			throw new InterruptedException("whichList should be -1, 0 or 1, here:"+whichList);
		}
	}
	
	public void clearDocScores(){
		docIDs_0.clear(); docScores_0.clear();
		docIDs_1.clear(); docScores_1.clear();
	}
	
	public void getRes(ArrayList<T> topDocs, ArrayList<Float> topScores) throws InterruptedException{
//		if (docIDs_1.size()!=0) {
//			System.out.println("docIDs_0:"+docIDs_0.size()+", docIDs_1:"+docIDs_1.size());
//		}
		General_IR.rank_get_TopDocScores_PriorityQueue(docIDs_0, docScores_0, topRank, topDocs, topScores, "DES", true, true);
		if (isConcateTwoList) {//add the second list 
			General_IR.rank_get_TopDocScores_PriorityQueue(docIDs_1, docScores_1, topRank, topDocs, topScores, "DES", true, false); //concate two rank
		}
	}
	
	public ArrayList<T> getRes_onlyRank() throws InterruptedException{
		ArrayList<T> topDocs=new ArrayList<T>(topRank*2); ArrayList<Float> topScores=new ArrayList<Float>(topRank*2);
		General_IR.rank_get_TopDocScores_PriorityQueue(docIDs_0, docScores_0, topRank, topDocs, topScores, "DES", true, true);
		if (isConcateTwoList) {//add the second list 
			General_IR.rank_get_TopDocScores_PriorityQueue(docIDs_1, docScores_1, topRank, topDocs, topScores, "DES", true, false); //concate two rank
		}
		return topDocs;
	}
}