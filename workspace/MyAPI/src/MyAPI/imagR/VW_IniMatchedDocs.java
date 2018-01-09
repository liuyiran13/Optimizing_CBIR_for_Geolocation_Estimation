package MyAPI.imagR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyCustomedHaoop.ValueClass.IntArr;

public class VW_IniMatchedDocs extends MulitLevelFileSys{
	Configuration conf;
	
	public VW_IniMatchedDocs(Configuration conf, String rootPath, int FileInter, boolean disp) {
		super(rootPath,FileInter,".seq",disp);
		this.conf=conf;
	}
	
	public int makeOne_VW_IniMatchedDocs(int vw, HashMap<Integer, ArrayList<Integer>> Q_Docs, boolean disp) throws IllegalArgumentException, IOException, InterruptedException{
		//outPut the matchedDocs into seq on vw_iniDocsPath
		SequenceFile.Writer VWFile_Writer=General_Hadoop.createSeqFileWriter(conf, new Path(getOneFilePath(vw)), IntWritable.class, IntArr.class);
		//--1st elemetn in the output is to mark vw!
		VWFile_Writer.append(new IntWritable(vw), new IntArr(new int[]{vw,vw,vw})); //key_vw, value_[vw,vw,vw]
		//--output query_docs
		int queryNum=0; int docNum=0;
		for (Entry<Integer, ArrayList<Integer>> oneQ_Docs : Q_Docs.entrySet()) {
			HashSet<Integer> checkDuplicate=new HashSet<Integer>(oneQ_Docs.getValue());
			General.Assert(checkDuplicate.size()==oneQ_Docs.getValue().size(), "err, duplicate docs in VW:"+vw+", Q:"+oneQ_Docs.getKey()+", its docs:"+oneQ_Docs.getValue());
			int[] docs=General.ListToIntArr(oneQ_Docs.getValue());
			Arrays.sort(docs);//sort docs in asc order
			VWFile_Writer.append(new IntWritable(oneQ_Docs.getKey()), new IntArr(docs)); //key_queryID, value_matchedDocs
			queryNum++;
			docNum+=oneQ_Docs.getValue().size();
		}
		VWFile_Writer.close();
		General.dispInfo_ifNeed(disp, "", "makeOne_VW_IniMatchedDocs finished, vw: "+vw+", total queryNum: "+queryNum+", matched photos:"+docNum);
		return docNum;
	}
	
	public int readVW_IniMatchedDocsIntoMemory(int vw, HashMap<Integer, int[]> QID_DIDs) throws IllegalArgumentException, IOException {
		Path vw_matchedDocsPath=new Path(getOneFilePath(vw));
		if (FileSystem.get(conf).exists(vw_matchedDocsPath)) {
			SequenceFile.Reader Reader=new SequenceFile.Reader(conf, SequenceFile.Reader.file(vw_matchedDocsPath));
			IntWritable TVector_key = new IntWritable(vw) ; //queryID
			IntArr TVector_value = new IntArr();//docID[]
			//******* check TVector's vw **************
			Reader.next(TVector_key, TVector_value);//1st element is to mark vw, value is not useful! 
			General.Assert(TVector_key.get()==vw, "err in readVW_MatchDocsIntoMemory: TVector's first element(vw) is:"+TVector_key.get()+", not vw:"+vw);
			//******* read TVector into memory **************
			QID_DIDs.clear();
			int DocNum=0; 
			while (Reader.next(TVector_key, TVector_value)) {
				QID_DIDs.put(TVector_key.get(), TVector_value.getIntArr());
				DocNum+=TVector_value.getIntArr().length;
			}
			Reader.close();
			return DocNum;
		}else{
			System.out.println("\n -Warning!!  for VW-"+vw+",  VW_IniMatchedDocs not exist for this vw! "+vw_matchedDocsPath);
			return -1;
		}
	}	
	

}
