package MyCustomedHaoop.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.Obj.Disp;
import MyAPI.imagR.Conf_ImageR;
import MyAPI.imagR.MakeRank;
import MyAPI.imagR.ScoreDoc;
import MyCustomedHaoop.Partitioner.Partitioner_forSearchTVector;
import MyCustomedHaoop.ValueClass.IntArr;
import MyCustomedHaoop.ValueClass.IntArr_FloatArr;
import MyCustomedHaoop.ValueClass.Int_Float;
import MyCustomedHaoop.ValueClass.VW_DID_Score_Arr;

public class Reducer_buildIniRank extends Reducer<IntWritable,VW_DID_Score_Arr,IntWritable,IntArr_FloatArr>  {
	
	Configuration conf;
	private ScoreDoc scoreDoc;
	private MakeRank<Integer> makeRank;
	
	private int maxIniRankLength;
	
	private boolean isDiffTopDocsByVW;
	private Path tempPathFor_VWDocs; //save vw_docs to seqFile on local node
	
	private int[] PaIDs;
	private int totRedNumForSearchTVector;
	private Partitioner_forSearchTVector<IntWritable> partitioner_forSearchTVector;
	
	private int processedQueryNum;
	private long startTime;
	private int dispInter;
	private boolean disp;
//	private boolean debug;
//	private int debug_queryID;
//	private int debug_docID;
	

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();	
		Conf_ImageR conf_ImageR=new Conf_ImageR(conf);
		//setup scoreDoc
		System.out.println("current memory:"+General.memoryInfo());
		scoreDoc= new ScoreDoc(new Disp(true, "\t", null), conf_ImageR); //do not need TVectorInfo for idf, as the score here alreay have idf in job2_2:Reducer_SearchTVector_getHMScore
		System.out.println("scoreDoc setup finished, current memory:"+General.memoryInfo());
		//-setup_makeRank
		isDiffTopDocsByVW=conf_ImageR.mr_isDiffTopDocsByVW;
		maxIniRankLength=conf_ImageR.mr_maxIniRankLength;
		makeRank= new MakeRank<Integer>(new Disp(true, "\t", null), maxIniRankLength, false, scoreDoc.indexInfo.maxDocID+1); //do not need concate, because only 1 type of ranking score
		System.out.println("makeRank setup finished, isDiffTopDocsByVW:"+isDiffTopDocsByVW+", maxIniRankLength:"+maxIniRankLength);
		//***** set tempPathFor_VWDocs for save vw_docs to SeqFile ***//
		if (isDiffTopDocsByVW) {
			tempPathFor_VWDocs= General_Hadoop.getLocalPath("vw_docs.seq", conf);
			System.out.println("tempPathFor_VWDocs for save vw_docs setted: "+tempPathFor_VWDocs);
			General.checkDir(new Disp(true, "", null), ".");
		}
		// ***** setup PaID for check duplicate-VW ***//
		PaIDs= (int[]) General.readObject(Conf_ImageR.sd_VWPaIDs);//each element in PaIDs is mutipled by 10!
		totRedNumForSearchTVector=General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs); //reducer number for seachTVector, PaIDs: values from 0!
		System.out.println("PaIDs set finished for check duplicate-VW, total partioned reducer number : "+totRedNumForSearchTVector+", job.setNumReduceTasks(jobRedNum) should >= this value!!");
		partitioner_forSearchTVector=new Partitioner_forSearchTVector<IntWritable>();
		partitioner_forSearchTVector.setConf(conf);
		//***** setup debug ************//
//		debug_queryID=-7001;
//		debug_docID=4553;
		// ***** setup finsihed ***//
		processedQueryNum=0;
		dispInter=5;
		disp=true;
		startTime=System.currentTimeMillis();
		System.out.println("setup finsihed!");
		
 	}
		
	@Override
	public void reduce(IntWritable QID, Iterable<VW_DID_Score_Arr> docs_scores_I, Context context) throws IOException, InterruptedException {
		/**
		 * 1 reduce: for 1 query, merge mutiple vw-list into one list, each list should be ordered in ASC by docID! 
		 */
		
		//key: query, value: vw and this vw-mathed docs and scores for this query

		int thisQueryID=QID.get();
//		if (QID.get()==debug_queryID) {
//			debug=true;
//			disp=true;
//		}
		//disp progress
		processedQueryNum++;
		if (processedQueryNum%dispInter==0){ 
			disp=true;
		}
		General.dispInfo_ifNeed(disp, "\n ", "this reduce is for "+processedQueryNum+" -th queries, QID:"+QID+", start process");
		
		
		//********1. combine all vw_docs_scores for one query, build rank ************
		float[] mergResu=new float[scoreDoc.indexInfo.maxDocID+1]; //need to merge many times for every vw's DID_Score list, use DID as index in mergResu, so DID should start from 0!
		SequenceFile.Writer seqFileWr_vw_Docs = isDiffTopDocsByVW?General_Hadoop.createSeqFileWriter(conf, tempPathFor_VWDocs, IntWritable.class, IntArr.class):null;
		int vwNum=0;  long startTimeInRed=System.currentTimeMillis(); HashSet<Integer> checkDupliVWs=new HashSet<Integer>();
//		LinkedList<Int_Float> debugOneDoc_vws_scores=new LinkedList<Int_Float>(); 
		for(Iterator<VW_DID_Score_Arr> it=docs_scores_I.iterator();it.hasNext();){
			VW_DID_Score_Arr one=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
			vwNum++;
			boolean showMergeDetail=(disp && vwNum%2000==0);
			//add vw
			int this_vw=one.vw;
			//save to local seqFile, each DID_Score_Arr should be ordered in ASC by docID! this is guanteed by: ASC docID in TVector
			if (isDiffTopDocsByVW) {
				seqFileWr_vw_Docs.append(new IntWritable(this_vw), new IntArr(one.getDocs()));
			}
			//merge
			if (showMergeDetail) {//show example for one Query
				System.out.println("\t this is "+vwNum+"-th mergeSortedList_ASC(mergResu, Arr), this merge is for vw:"+this_vw+", before merge:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
		    	System.out.println("\t --list size:"+one.docs_scores.getArr().length+", top docs:"+General.selectArr(one.docs_scores.getArr(), null, 5));
		    	System.out.println("\t --mergResu topID docs:"+General.floatArrToString(General.selectArrFloat(mergResu, null, 5), "_", "0.0000") );
		    	System.out.println("\t --current memory info: "+General.memoryInfo());
			}
			General_IR.mergeSortedList_ASC(mergResu, one.docs_scores.getArr());
			if (showMergeDetail) {//show example for one Query
				System.out.println("\t "+vwNum+"-th mergeSortedList_ASC(mergResu, Arr) finished: "+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
		    	System.out.println("\t --mergResu topID docs:"+General.floatArrToString(General.selectArrFloat(mergResu, null, 5), "_", "0.0000") );
			}
			//checkDupliVWs 
			General.Assert(checkDupliVWs.add(this_vw), "err! duplicate VWs for query:"+thisQueryID+", duplicate vw:"+this_vw
					+", its allocated reducer num when searchTVector:"+General_Hadoop.getVWReducerNum_from_vwPartitionIDs(PaIDs, this_vw)
					+", partitionID:"+partitioner_forSearchTVector.getPartition(new IntWritable(this_vw), QID, totRedNumForSearchTVector));
			//debug for one query, doc
//			if (debug) {
//				for (DID_Score did_score : one.docs_scores.getArr()) {
//					if (did_score.docID==debug_docID) {
//						debugOneDoc_vws_scores.add(new Int_Float(this_vw, did_score.score));
//					}
//				}
//			}
		}
		if (isDiffTopDocsByVW) {
			seqFileWr_vw_Docs.close();
		}
		if (disp){//show example for one Query
			System.out.println("\t all mergeSortedList_ASC(mergResu, Arr) finished, vwNum: "+vwNum+", time:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
			System.out.println("\t --mergResu topID docs:"+General.floatArrToString(General.selectArrFloat(mergResu, null, 5), "_", "0.0000") );
	    	System.out.println("\t --current memory info: "+General.memoryInfo());
		}
		//**************** find top ranked docs, and output them by vw, each vw has socred docs, and some of these are top ranked docs ****************
		int dispInter=mergResu.length/10+1;
		for (int docID = 0; docID < mergResu.length; docID++) {//docID is the index!
			if (mergResu[docID]>0) {//this docID has match score
				makeRank.addOneDoc_onlyMainRank(docID, mergResu[docID]/scoreDoc.indexInfo.docNorms[docID]);//final doc score should be normalised!!
			}
			//show info
			if (disp && (docID%dispInter==0)) {
				System.out.println("\t result in mergResu: docID:"+docID+", HE score:"+mergResu[docID]+", final normalised:"+mergResu[docID]/scoreDoc.indexInfo.docNorms[docID]);
			}
		}
		int scoredDocNum=makeRank.getScoredDocNum(0);
		//find top ranked docs
		ArrayList<Integer> docs_top=new ArrayList<Integer>(maxIniRankLength*2); ArrayList<Float> scores_top=new ArrayList<Float>(maxIniRankLength*2); 
		makeRank.getRes(docs_top, scores_top);
		makeRank.clearDocScores();
		//debug
//		if (debug) {
//			int docID=debug_docID;
//			System.err.println("debugInfo for QID:"+QID);
//			System.err.println("one docID:"+docID+", HE score:"+mergResu[docID]+", doc_BoVWVectorNorm: "+scoreDoc.doc_BoVWVectorNorm[docID]+", final normalised:"+mergResu[docID]/scoreDoc.doc_BoVWVectorNorm[docID]);
//			System.err.println("its matched vws:"+debugOneDoc_vws_scores.size()+", detailed vws_scores:"+debugOneDoc_vws_scores);
//			System.err.println("\t selected top docs:"+docs_top.size()+", top10 examples in docs_top: "+docs_top.subList(0, Math.min(docs_top.size(),10))
//					+", scores_top: "+scores_top.subList(0, Math.min(scores_top.size(),10)));
//		}
		
		
		
		//***********2. output rank or group docs by matched VW ***********
		if (!isDiffTopDocsByVW) {//output rank
			context.write(QID, new IntArr_FloatArr(docs_top,scores_top));
		}else {//output vw separated docs
			//sort top ranked docs by docID in ASC!
			ArrayList<Int_Float> docsRank_Score=new ArrayList<Int_Float>(docs_top.size()*2); ArrayList<Integer> docIDs=new ArrayList<Integer>(docs_top.size()*2); 
			for (int i = 0; i < docs_top.size(); i++) {
				docsRank_Score.add(new Int_Float(i, scores_top.get(i)));
				docIDs.add(docs_top.get(i));//use docID as ranking score
			}
			ArrayList<Int_Float> docsRankScore_sortbyDocID=new ArrayList<Int_Float>(docs_top.size()*2); ArrayList<Integer> sortedDocIDs=new ArrayList<Integer>(docs_top.size()*2); 
			General_IR.rank_get_AllSortedDocIDs_treeSet(docsRank_Score, docIDs, docsRankScore_sortbyDocID, sortedDocIDs, "ASC");
			int[] topDocs_inSortedDocIDs=General.ListToIntArr(sortedDocIDs);
			General.dispInfo_ifNeed(disp, "\t DiffTopDocsByVW: ", "selected top docs:"+sortedDocIDs.size()+", top examples in sortedDocIDs: "+sortedDocIDs.subList(0, Math.min(sortedDocIDs.size(),10))
						+", time:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
			//find topDocs' involved vws
			SequenceFile.Reader seqFileRe_vw_Docs=new SequenceFile.Reader(conf, SequenceFile.Reader.file(tempPathFor_VWDocs));
			vwNum=0; int commonNumTot=0;  IntWritable local_key=new IntWritable(); IntArr local_value=new IntArr(); int vwNum_existInTopDocs=0;
			while (seqFileRe_vw_Docs.next(local_key, local_value)) {
				vwNum++;
				boolean debugShow=(disp && vwNum%2000==0);
				int thisVW=local_key.get();
				int[] thisDocs=local_value.getIntArr();//must be ordered based on docID in ASC!
				ArrayList<int[]> commons=General.findCommonElementInds_twoSorted_ASC_loopShotArr(topDocs_inSortedDocIDs, thisDocs);//find common elements in two sorted arr
				if (commons!=null) {
					HashSet<Integer> checkDuplicate=new HashSet<Integer>();
					for (int[] one : commons) {
						Int_Float rank_score=docsRankScore_sortbyDocID.get(one[1]);
						context.write(new IntWritable(thisVW), new IntArr_FloatArr(new int[]{thisQueryID, one[0], rank_score.integerV}, new float[]{rank_score.floatV})); //key_vw, value_QID_DID_rank, score
						General.Assert(checkDuplicate.add(one[0]), "err! duplicate docs in commons! vw:"+thisVW+", query:"+thisQueryID
								+", duplicate doc:"+one[0]+", rank in topDocs:"+one[1]);
					}
					General.Assert(checkDuplicate.size()==commons.size(), "err! duplicate docs in commons! checkDuplicate.size():"+checkDuplicate.size()+", commons.size():"+commons.size());
					commonNumTot+=commons.size();
					vwNum_existInTopDocs++;
				}
				//debug show common elements
				if (debugShow) {//for debug show
					System.out.println("\t DiffTopDocsByVW: "+vwNum+"-th vw(finding common docs), time:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms")+", thisVW: "+thisVW+", top in thisDocs: "+General.IntArrToString(General.selectArrInt(thisDocs, null, Math.min(thisDocs.length,10)),"_"));
					if (commons!=null) {
						System.out.println("\t DiffTopDocsByVW: this vw:"+thisVW+", commonDocs: "+commons.size());
					}else{
						System.out.println("\t DiffTopDocsByVW: this vw:"+thisVW+", commonDocs: "+0);
					}
				}
			}
			//clean-up seqFile in local node
			seqFileRe_vw_Docs.close();
			General.Assert(FileSystem.getLocal(conf).delete(tempPathFor_VWDocs,true), "err in delete tempPathFor_VWDocs, not successed!") ;
			//show example for one Query
			General.dispInfo_ifNeed(disp, "\t DiffTopDocsByVW: ", "initial matchedVWnum:"+vwNum+", vwNum_existInTopDocs:"+vwNum_existInTopDocs+", total common docs num between topDocs and scoredDocs:"+commonNumTot+", on average, "+(float)commonNumTot/vwNum_existInTopDocs+" common docs per vw");
		}
		if (disp){//show example for one Query
			int topToShow=Math.min(docs_top.size(),10);
			System.out.println(processedQueryNum+" querys finished! time:"+General.dispTime(System.currentTimeMillis()-startTime, "min")
					+", current finished queryName: "+thisQueryID
					+", matched vwNum:"+vwNum+", total listed photos in its initial rank: "+scoredDocNum+", saved top doc numbers:"+docs_top.size()
					+", top ranked Docs:"+docs_top.subList(0, topToShow)+", Scores: "+scores_top.subList(0, topToShow));
	    	disp=false;
		}
		//debug
//		if (debug) {
//			throw new InterruptedException("this is the target query!");
//		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("\n one Reducer finished! total querys in this reducer:"+processedQueryNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
		// ***** finsihed ***//			
		
 	}
}
