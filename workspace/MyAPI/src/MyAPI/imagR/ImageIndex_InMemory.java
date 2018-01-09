package MyAPI.imagR;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import MyAPI.General.General;
import MyAPI.Obj.DataInOutput_Functions;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.ArrWritableClassCollection.TVector_Arr;
import MyCustomedHaoop.ValueClass.SURFfeat_ShortArr_AggSig;
import MyCustomedHaoop.ValueClass.TVector;

public class ImageIndex_InMemory extends ImageIndex{//TVectors are in-memory

	//********************** data ******************
	private TVector_Arr tVectors; 
		
	public ImageIndex_InMemory(String iniR_Scheme) {
		indexInfo=new ImageIndexInfo(iniR_Scheme);
	}
	
	public void buildIndexFromFeat(int vwNum, HashMap<Integer, Integer> L_to_S, ImageDataManager docData_IDInL, String indexPath, Disp disp) throws InterruptedException, IOException{
		long startTime=System.currentTimeMillis();
		indexPath=getFullIndexPath(indexPath);
		if (new File(indexPath).exists()) {
			disp.disp("indexPath is exist, no need to build, just load it from indexPath:"+indexPath);
			loadIndexFromDisk(indexPath);
			disp.disp("index loaded from indexPath:"+indexPath+", "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", Memory: "+General.memoryInfo());
			disp.disp("stat_index: "+indexInfo.getIndexStat());
		}else{
			disp.disp("indexPath:"+indexPath+" is not exist, start buildIndexFromFeat!");
			//make TVectors
			tVectors=new TVector_Arr(vwNum);
			int doc_i=0; int totDocNum=0;
			for (Entry<Integer, Integer> docID_L_to_S : L_to_S.entrySet()) {
				PhotoAllFeats_orgVW feat=docData_IDInL.getPhoFeat(docID_L_to_S.getKey(), Disp.getNotDisp());
				if (feat!=null) {
					buildIndexFromFeat_addOneDoc(docID_L_to_S.getValue(), feat);
					totDocNum++;
				}else{
					disp.disp("buildIndexFromFeat, make docInfos: no feat for docID_L_to_S:"+docID_L_to_S+", isExistImage:"+(docData_IDInL.getImage(docID_L_to_S.getKey(), Disp.getNotDisp())!=null));
				}
				disp.disp(1000, doc_i, "make TVectors: "+doc_i+"-th doc finished! current feated totDocNum: "+totDocNum+", "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", Memory: "+General.memoryInfo());
				doc_i++;
			}
			disp.disp("make TVectors done! "+doc_i+" docs finished! feated totDocNum: "+totDocNum+", "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", Memory: "+General.memoryInfo());
			indexInfo.makeIDF_Squre(tVectors,totDocNum);
			//make docInfos, this step needs idf_squre, so must first run make TVectors, and getIDF_Squre
			LinkedList<DID_DocInfo> did_docInfos=new LinkedList<>();
			doc_i=0; 
			for (Entry<Integer, Integer> docID_L_to_S : L_to_S.entrySet()) {
				PhotoAllFeats_orgVW feat=docData_IDInL.getPhoFeat(docID_L_to_S.getKey(), Disp.getNotDisp());
				//docInfo
				if (feat!=null) {
					did_docInfos.add(new DID_DocInfo(docID_L_to_S.getValue(), feat.getDocInfo(indexInfo.idf_squre)));
				}
				disp.disp(1000, doc_i, "make docInfos: "+doc_i+"-th doc finished! "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", Memory: "+General.memoryInfo());
				doc_i++;
			}
			indexInfo.getDocSideInfo(did_docInfos);
			//done and save
			disp.disp("done! buildIndexFromFeat, total docs: "+doc_i+", "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", before trimToSize Memory: "+General.memoryInfo());
			disp.disp("stat_index: "+indexInfo.getIndexStat());
			tVectors.sortByDocID(true);
			disp.disp("done! trimToSize TVector, "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", after trimToSize Memory: "+General.memoryInfo());
			saveIndexOnDisk(indexPath, did_docInfos);
			disp.disp("index saved to indexPath:"+indexPath+", "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		}
	}
	
	private void buildIndexFromFeat_addOneDoc(int docID, PhotoAllFeats_orgVW featArr){
		//TVector
		for (Entry<Integer, SURFfeat_ShortArr_AggSig> oneVW_Feats : featArr.VW_Sigs.entrySet()) {
			tVectors.addOneDoc_featArr(oneVW_Feats.getKey(), docID, oneVW_Feats.getValue().feats, oneVW_Feats.getValue().aggSig);
		}		
	}
	
	private String getFullIndexPath(String indexPath){
		return indexPath+".imgIndex";
	}
	
	public void saveIndexOnDisk(String path, LinkedList<DID_DocInfo> did_docInfos) throws FileNotFoundException, IOException{
		//save: TVector, DocInfo
		DataOutputStream outPut = new DataOutputStream( new BufferedOutputStream(new FileOutputStream(path)));
		tVectors.write(outPut);
		DataInOutput_Functions.writeList(did_docInfos, outPut);
		outPut.close();
	}
	
	public void loadIndexFromDisk(String path) throws IOException, InterruptedException {
		DataInputStream inPut = new DataInputStream( new BufferedInputStream(new FileInputStream(path)));
		//load TVector
		tVectors=new TVector_Arr();
		tVectors.readFields(inPut);
		//make indexInfo from TVector and docInfo
		LinkedList<DID_DocInfo> did_docInfos=DataInOutput_Functions.readList(inPut, DID_DocInfo.class);
		indexInfo.makeIDF_Squre(tVectors,did_docInfos.size());
		indexInfo.getDocSideInfo(did_docInfos);
		inPut.close();
	}

	@Override
	public TVector getOneTVector(int vw) {
		return tVectors.getOneTVector(vw);
	}
}
