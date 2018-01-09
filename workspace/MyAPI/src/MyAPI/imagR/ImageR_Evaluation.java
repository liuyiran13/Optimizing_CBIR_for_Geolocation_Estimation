package MyAPI.imagR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.Geo.GVM_Evaluator_Loc;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.IntList_FloatList;

public class ImageR_Evaluation {
	
	public class EvaluationMatric{
		float AP;
		boolean cartoCorrect;
		
		public EvaluationMatric(float AP, boolean cartoCorrect){
			this.AP=AP;
			this.cartoCorrect=cartoCorrect;
		}
	}
	
	//dataset
	String indexLabel;
	String rankLabel;
	boolean isOxfordBuilding;
	boolean isBarcelonaBuilding;
	boolean isHerve;
	boolean isSanFran;
	boolean isME15;
	HashMap<Integer, HashSet<Integer>> groundTrue;
	HashMap<Integer, HashSet<Integer>> junks;
	HashMap<Integer, HashSet<Integer>> buildingInd_Name;
	
	String dataSetInfo;
	int queryNum_inGroundTru;
	int queryNum_existRank;
	//***** evaluate matircs
	boolean isGTExist;//if GTruth exist, then can use MAP to evaluate, 
	boolean isLatlonEva;//eval use precision at each rank level (ME15 location).
	//1. evaluate matirc: MAP
	float MAP; 
	float HR_1; 
	//pairwise PRCurve
	boolean isNeedPRCurve;
	float[] thresholds;
	int tot_TruePositive;
	float[][] truePositive_totPositive;
	//2. evaluate matirc: latlon
	GVM_Evaluator_Loc latlonEva;//eval use precision at each rank level (ME15 location).
	
	public ImageR_Evaluation(Disp disp, Conf_ImageR conf_ImageR) throws InterruptedException{
		this(disp, conf_ImageR.ev_queryNum, conf_ImageR.ds_indexLabel, conf_ImageR.mr_rankLabel, Conf_ImageR.ev_groundTrue, Conf_ImageR.ev_junks, Conf_ImageR.ev_buildingInd_Name, conf_ImageR.ev_latlonEvaFlag, Conf_ImageR.ev_latlons, conf_ImageR.ev_thresholdsForPRCurve);
	}
	
	@SuppressWarnings("unchecked")
	public ImageR_Evaluation(Disp disp, int queryNum_inGroundTru, String indexLabel, String rankLabel, 
			String grondTruthPath, String junkPath, String buildingInd_NamePath, 
			String latlonEvaFlag, String latlonsPath, 
			String thresholdsInString) throws InterruptedException{
		//note: must gurantee docID in rank and grondTruth is the same, support use s_to_l_Path if rankDocId is in S and grondTruth is in L
		this.queryNum_inGroundTru=queryNum_inGroundTru;
		this.indexLabel=indexLabel;
		this.rankLabel=rankLabel;
		dataSetInfo="indexLabel:"+indexLabel+", rankLabel:"+rankLabel+", grondTruthPath:"+grondTruthPath+", junkPath:"+junkPath+", buildingInd_NamePath:"+buildingInd_NamePath;
		groundTrue=(HashMap<Integer, HashSet<Integer>>) General.readObject(grondTruthPath);
		if (groundTrue!=null) {
			isGTExist=true;
			dataSetInfo+=", groundTrue is loaded";
		}else {
			isGTExist=false;
			dataSetInfo+=", groundTrue is not loaded";
		}
		if (latlonEvaFlag!=null) {
			latlonEva=new GVM_Evaluator_Loc(latlonEvaFlag, latlonsPath);//noDivideQ@1,3,5,10_0.001,0.01,0.1
			General.Assert(latlonEva.workModel==1, "err! in imageR, only support GVM_Evaluator.workModel==1");
			isLatlonEva=true;
			dataSetInfo+=", latlonEva is built";
		}
		if (indexLabel.startsWith("_Oxford")) {//Oxford dataset
			isOxfordBuilding=true;
			junks=(HashMap<Integer, HashSet<Integer>>) General.readObject(junkPath);
			buildingInd_Name=(HashMap<Integer, HashSet<Integer>>) General.readObject(buildingInd_NamePath);
			dataSetInfo+=(junks==null)?", junks is not loaded":", junks is loaded";
			dataSetInfo+=(buildingInd_Name==null)?", buildingInd_Name is not loaded":", buildingInd_Name is loaded";
		}else if(indexLabel.startsWith("_Barcelona")){
			isBarcelonaBuilding=true;
		}else if(indexLabel.startsWith("_Herve")){
			isHerve=true;
		}else if(indexLabel.startsWith("_SanFran")){
			isSanFran=true;
		}else if(indexLabel.startsWith("_ME15")){
			isME15=true;
		}else {
			throw new InterruptedException("indexLabel should contains Oxford, Barcelona, Herve, SanFran or ME15");
		}
		//pairwise PRCurve
		this.isNeedPRCurve=(thresholdsInString!=null && !thresholdsInString.isEmpty());
		if(isNeedPRCurve){
			this.thresholds=General.StrArrToFloatArr(thresholdsInString.split(","));
			tot_TruePositive=0;
			for (HashSet<Integer> oneGroundTrue : groundTrue.values()) {
				tot_TruePositive+=Math.pow(oneGroundTrue.size(), 2);
			}			
			dataSetInfo+="\n\t isNeedPRCurve:"+isNeedPRCurve+", for pairwise PRCurve, tot_TruePositive:"+tot_TruePositive+", thresholds: "+thresholdsInString;
		}
		//ini_ImageR_Evaluation
		ini_ImageR_Evaluation();
		//disp
		disp.disp("setup ImageR_Evaluation finished, "+getDataSetInfo());
	}

	public String getDataSetInfo(){
		return "In Evaluation, "+dataSetInfo;
	}
	
	public void ini_ImageR_Evaluation(){
		//evaluate matirc
		queryNum_existRank=0; MAP=0; HR_1=0; 
		if (isLatlonEva) {
			latlonEva.clearData();
		}
		if(isNeedPRCurve){
			truePositive_totPositive=new float[thresholds.length][];
			//ini truePositive_totPositive
			for (int i = 0; i < truePositive_totPositive.length; i++) {
				truePositive_totPositive[i]=new float[2];
			}
		}
	}
	
	public void ini_ImageR_Evaluation(String rankLabel){
		this.rankLabel=rankLabel;
		ini_ImageR_Evaluation();
	}
	
	public String analysisOneMapFile(String mapFilePath, long startTime) throws IOException{
		ini_ImageR_Evaluation();
		MapFile.Reader rankMapFile=General_Hadoop.openOneMapFile(mapFilePath);
		IntWritable Key_queryName= new IntWritable();
	 	IntList_FloatList Value_RankScores= new IntList_FloatList();
		while (rankMapFile.next(Key_queryName, Value_RankScores)) {
			add_oneRank(Key_queryName.get(), Value_RankScores, Disp.getNotDisp());			
		}
		rankMapFile.close();
		return getDataSetInfo()+"\n\t"+getRes()+"  ......  "+General.dispTime(System.currentTimeMillis()-startTime, "min");	
	}
	
	public void add_oneRank(int queryName, IntList_FloatList oneRank, Disp disp){
		//******* transfer rank's docID to oriID *********************
		ArrayList<Integer> oriIDs=oneRank.getIntegers();
		//******* analysis one query's result *********************
		String queryInfoToShow="current is "+queryNum_existRank+"-th query, queryName:"+queryName;
		queryNum_existRank++;
		if (isGTExist) {
			//1. MAP
			HashSet<Integer> relPhos=null;
			if (isOxfordBuilding) {
				int buildingInd=queryName/1000;//oxford use 1000 to group
				relPhos=groundTrue.get(buildingInd);
				General.dispInfo_ifNeed(relPhos==null, "", "err! relPhos==null, queryName:"+queryName+", buildingInd:"+buildingInd+", groundTrue:"+groundTrue);
				oriIDs.removeAll(junks.get(buildingInd));//remove junks
				queryInfoToShow+=", buildingName:"+buildingInd_Name.get(buildingInd);
			}else if (isBarcelonaBuilding){
				int buildingInd=queryName/10000;//Barcelona use 10000 to group
				relPhos=groundTrue.get(buildingInd);
				General.dispInfo_ifNeed(relPhos==null, "", "err! relPhos==null, queryName:"+queryName+", buildingInd:"+buildingInd+", groundTrue:"+groundTrue);
				queryInfoToShow+=", buildingName:"+buildingInd;
			}else {
				relPhos=groundTrue.get(queryName);
			}
			float AP=General_IR.AP_smoothed(relPhos, oriIDs);
			MAP+=AP;
			//2. HR_1
			boolean cartoCorrect=relPhos.contains(oriIDs.get(0));
			HR_1+=cartoCorrect?1:0;	
			//3. PRCurve
			if (isNeedPRCurve) {
				General.elementAdd_saveInA(truePositive_totPositive, General_IR.PR_Curve(relPhos, oriIDs, oneRank.getFloats(), thresholds, false));
			}
			disp.disp(queryInfoToShow+", AP:"+AP+", 1NN-cartoCorrect:"+cartoCorrect+", MAP:"+MAP/queryNum_existRank+", Mean_1NN-cartoCorrect:"+(float)HR_1/queryNum_existRank);
		}
		if (isLatlonEva) {
			//4. latlonEval
			latlonEva.addOneQueryRank(queryName, latlonEva.evalOneQuery_doc(queryName, oneRank.getIntegers()), 0,0);
		}
	}
	
	public String getRes() {
		//outPut as String
		StringBuffer outInfo=new StringBuffer();
		outInfo.append("indexLabel:"+indexLabel+", rankLabel: "+rankLabel+"\n");
		outInfo.append("\t should have tot "+queryNum_inGroundTru+" querys, no rank query num:"+(queryNum_inGroundTru-queryNum_existRank)+"\n");//should use queryNum in groundTrue for MAP calculation
		if (isGTExist) {
			outInfo.append("MAP:"+MAP/queryNum_inGroundTru+", HR_1:"+HR_1/queryNum_inGroundTru);//should use queryNum in groundTrue for MAP calculation
			//make pairwise PRCurve
			if (isNeedPRCurve) {
				outInfo.append(", pairwise PRCurve: ");
				for (float[] one : truePositive_totPositive) {
					outInfo.append(one[0]/one[1]+"_"+one[0]/tot_TruePositive+", ");
				}
			}
		}
		if (isLatlonEva) {
			outInfo.append(latlonEva.getEvalationRes(queryNum_inGroundTru));//should use queryNum in groundTrue for calculation
		}
		
		return outInfo.toString();
	}
}
