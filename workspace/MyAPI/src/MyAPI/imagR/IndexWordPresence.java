package MyAPI.imagR;

import java.util.ArrayList;
import MyAPI.General.General;

public class IndexWordPresence {

	public ArrayList<ArrayList<Integer>> invertedIndex;
	
	public IndexWordPresence(int wordNum, int maxDocNum) {
		invertedIndex=General.ini_ArrayList_ArrayList(wordNum,maxDocNum);
	}
	
	public void addOneDocWord(int docID, int wordID){
		invertedIndex.get(wordID).add(docID);
	}
	
	public int getWordNum(){
		return invertedIndex.size();
	}
	
	public ArrayList<Integer> getOneWordPresence(int wordID){
		return invertedIndex.get(wordID);
	}

}
