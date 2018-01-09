package MyAPI.imagR;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.PGMSource;
import MyAPI.Interface.FeatInd;
import MyAPI.Interface.I_HESig;
import MyAPI.Obj.Disp;
import MyAPI.Obj.HistMultiD_Sparse_equalSizeBin_forFloat;
import MyAPI.Obj.Hist_forFloat;
import MyAPI.Obj.Pair_int;
import MyAPI.Obj.SelectID;
import MyCustomedHaoop.ValueClass.ArrWritableClassCollection.MatchFeat_Arr;
import MyCustomedHaoop.ValueClass.DID_Score;
import MyCustomedHaoop.ValueClass.DocAllMatchFeats;
import MyCustomedHaoop.ValueClass.ImageRegionMatch;
import MyCustomedHaoop.ValueClass.Int_MatchFeatArr;
import MyCustomedHaoop.ValueClass.Int_SURFfeat_ShortArr;
import MyCustomedHaoop.ValueClass.MatchFeat;
import MyCustomedHaoop.ValueClass.SURFfeat;
import MyCustomedHaoop.ValueClass.SURFfeat_ShortArr_AggSig;
import MyCustomedHaoop.ValueClass.SURFpoint;
import MyCustomedHaoop.ValueClass.TVector;

public class ScoreDoc {
	
	public class MatchingInfo {
		int mathchNum_max;
		LinkedList<String> ignored_MachNums;
		public int scoreDocNum;
		public long scoreDocTime_tot;
		public long scoreDocTime_1vs1;//for 1vs1 
		public long scoreDocTime_HV;//for HV 
		public long scoreDocTime_PG;//for PG 
		//matchNum Vs. process-time
		public long[] matchNum_time_1vs1;
		public long[] matchNum_time_HV;
		public long[] matchNum_time_PG;
		public int[] matchNum_vote_1vs1;
		public int[] matchNum_vote_HV;
		public int[] matchNum_vote_PG;
		//match-num reduced ratio
		public Hist_forFloat<Integer> hist_matchNumSel_1vs1;
		public Hist_forFloat<Integer> hist_matchNumSel_HV;
		public Hist_forFloat<Integer> hist_matchNumSel_1vs1_HV;
		public Hist_forFloat<Integer> hist_matchNumSel_PG;
		public Hist_forFloat<Integer> hist_matchNumSel_1vs1_HV_PG;
		
		public MatchingInfo() {
			mathchNum_max=30000;//max mathchNum: 30k 
			ignored_MachNums=new LinkedList<String>();
			matchNum_time_1vs1=new long[mathchNum_max];
			matchNum_time_HV=new long[mathchNum_max];
			matchNum_time_PG=new long[mathchNum_max];
			matchNum_vote_1vs1=new int[mathchNum_max];
			matchNum_vote_HV=new int[mathchNum_max];
			matchNum_vote_PG=new int[mathchNum_max];
			hist_matchNumSel_1vs1=new Hist_forFloat<Integer>(false); hist_matchNumSel_1vs1.makeEqualBins(0, 1, (float) 0.1, "0.0%"); hist_matchNumSel_1vs1.iniHist();
			hist_matchNumSel_HV=new Hist_forFloat<Integer>(false); hist_matchNumSel_HV.makeEqualBins(0, 1, (float) 0.1, "0.0%"); hist_matchNumSel_HV.iniHist();
			hist_matchNumSel_1vs1_HV=new Hist_forFloat<Integer>(false); hist_matchNumSel_1vs1_HV.makeEqualBins(0, 1, (float) 0.1, "0.0%"); hist_matchNumSel_1vs1_HV.iniHist();
			hist_matchNumSel_PG=new Hist_forFloat<Integer>(false); hist_matchNumSel_PG.makeEqualBins(0, 1, (float) 0.1, "0.0%"); hist_matchNumSel_PG.iniHist();
			hist_matchNumSel_1vs1_HV_PG=new Hist_forFloat<Integer>(false); hist_matchNumSel_1vs1_HV_PG.makeEqualBins(0, 1, (float) 0.1, "0.0%"); hist_matchNumSel_1vs1_HV_PG.iniHist();
		}
		
		public int add_1vs1(long startTime, int matchNum_before1vs1, int matchNum_after1vs1){
			int time_1vs1=(int) (System.currentTimeMillis()-startTime);
			if (matchNum_before1vs1<mathchNum_max) {
				matchNum_time_1vs1[matchNum_before1vs1]+=time_1vs1;
				matchNum_vote_1vs1[matchNum_before1vs1]++;
				hist_matchNumSel_1vs1.addOneSample((float)matchNum_after1vs1/matchNum_before1vs1, matchNum_before1vs1);
				//add tot time
				scoreDocTime_tot+=time_1vs1;
				scoreDocTime_1vs1+=time_1vs1;
			}else {
				ignored_MachNums.add("before1vs1:"+matchNum_before1vs1);
				System.out.println("warning in MatchingInfo.add_1vs1: current matchNum_before1vs1:"+matchNum_before1vs1+", is too big! mathchNum_max allowed:"+mathchNum_max+", ignore this one!");
			}
			return time_1vs1;
		}
		
		public int add_HV(long startTime, int matchNum_beforeHV, int matchNum_afterHV){
			int time_HV=(int) (System.currentTimeMillis()-startTime);
			if (matchNum_beforeHV<mathchNum_max) {
				matchNum_time_HV[matchNum_beforeHV]+=time_HV;
				matchNum_vote_HV[matchNum_beforeHV]++;
				hist_matchNumSel_HV.addOneSample((float)matchNum_afterHV/matchNum_beforeHV, matchNum_beforeHV);
				//add tot time
				scoreDocTime_tot+=time_HV;
				scoreDocTime_HV+=time_HV;
			}else {
				ignored_MachNums.add("beforeHV:"+matchNum_beforeHV);
				System.out.println("warning in MatchingInfo.add_HV: current matchNum_beforeHV:"+matchNum_beforeHV+", is too big! mathchNum_max allowed:"+mathchNum_max+", ignore this one!");
			}
			return time_HV;
		}
		
		public void add_1vs1_HV(int matchNum_before1vs1_HV, int matchNum_after1vs1_HV){
			hist_matchNumSel_1vs1_HV.addOneSample((float)matchNum_after1vs1_HV/matchNum_before1vs1_HV, matchNum_before1vs1_HV);
		}
		
		public int add_PG(long startTime, int matchNum_beforePG, int matchNum_afterPG){
			int time_PG=(int) (System.currentTimeMillis()-startTime);
			if (matchNum_beforePG<mathchNum_max) {
				matchNum_time_PG[matchNum_beforePG]+=time_PG;
				matchNum_vote_PG[matchNum_beforePG]++;
				hist_matchNumSel_PG.addOneSample((float)matchNum_afterPG/matchNum_beforePG, matchNum_beforePG);
				//add tot time
				scoreDocTime_tot+=time_PG;
				scoreDocTime_PG+=time_PG;
			}else {
				ignored_MachNums.add("beforePG:"+matchNum_beforePG);
				System.out.println("warning in MatchingInfo.add_PG: current matchNum_beforePG:"+matchNum_beforePG+", is too big! mathchNum_max allowed:"+mathchNum_max+", ignore this one!");
			}
			return time_PG;
		}
		
		public void add_1vs1_HV_PG(int matchNum_before1vs1_HV_PG, int matchNum_after1vs1_HV_PG){
			hist_matchNumSel_1vs1_HV_PG.addOneSample((float)matchNum_after1vs1_HV_PG/matchNum_before1vs1_HV_PG, matchNum_before1vs1_HV_PG);
		}
	
		public String getComputingTimeReport(){
			DecimalFormat format=new DecimalFormat("0.00");
			StringBuffer info=new StringBuffer();
			info.append("ComputingTimeReport: total doc num:"+scoreDocNum+"\n");
			//ignored_MachNums
			info.append("mathchNum_max:"+mathchNum_max+ignored_MachNums.size()+" ignored_MachNums: "+ignored_MachNums+"\n");
			//overall time
			info.append("\t scoreDocTime_tot:"+General.dispTime(scoreDocTime_tot,"ms")+", on average per doc:"+(float)scoreDocTime_tot/scoreDocNum+" ms"+"\n");
			info.append("\t scoreDocTime_1vs1:"+General.dispTime(scoreDocTime_1vs1,"ms")+", on average per doc:"+(float)scoreDocTime_1vs1/scoreDocNum+" ms"+"\n");
			info.append("\t scoreDocTime_HV:"+General.dispTime(scoreDocTime_HV,"ms")+", on average per doc:"+(float)scoreDocTime_HV/scoreDocNum+" ms"+"\n");
			info.append("\t scoreDocTime_PG:"+General.dispTime(scoreDocTime_PG,"ms")+", on average per doc:"+(float)scoreDocTime_PG/scoreDocNum+" ms"+"\n");
			//matchNum_time
			info.append("\t matchNum_time in matchNum_1vs1_HV_PG: ");
			for (int i = 0; i < matchNum_time_1vs1.length; i++) {
				if (matchNum_time_1vs1[i]!=0) {
					info.append(i+"_"+format.format((float)matchNum_time_1vs1[i]/matchNum_vote_1vs1[i])
							+"_"+format.format((float)matchNum_time_HV[i]/matchNum_vote_HV[i])
							+"_"+format.format((float)matchNum_time_PG[i]/matchNum_vote_PG[i])+", ");
				}
			}
			info.append("\n");
			//hist_matchNumReduce
			info.append("hist_matchNumSel_1vs1: "+hist_matchNumSel_1vs1.makeRes("0.0%", false)+"\n");
			info.append("hist_matchNumSel_HV: "+hist_matchNumSel_HV.makeRes("0.0%", false)+"\n");
			info.append("hist_matchNumSel_1vs1_HV: "+hist_matchNumSel_1vs1_HV.makeRes("0.0%", false)+"\n");
			info.append("hist_matchNumSel_PG: "+hist_matchNumSel_PG.makeRes("0.0%", false)+"\n");
			info.append("hist_matchNumSel_1vs1_HV_PG: "+hist_matchNumSel_1vs1_HV_PG.makeRes("0.0%", false)+"\n");
			return info.toString();
		}
	}

	//config for initial retrieval step
	public boolean iniR_noBurst; 
	public boolean iniR_BurstIntra; 
	public boolean iniR_BurstInter; 
	public boolean iniR_BurstIntraInter; 
	public boolean iniR_1vw1match;
	public boolean iniR_ASMK;
	public String scoreFlag;
	public ImageIndexInfo indexInfo;
	public HEParameters HEPara_iniR;
	public HEParameters HEPara_reR;
	public double ASMK_thr;
	public double ASMK_alpa;
	public double[] ASMK_weight;
	public boolean isOnlyUseHMDistFor1Vs1;
	public boolean is1vw1match;
	public boolean isUpRightFeat;
	public int HPM_ParaDim; //for HPM
	public int HPM_level; //for HPM
	public double[][] scalingInfo_min_max; //for HPM
	public HistMultiD_Sparse_equalSizeBin_forFloat<Pair_int> hist_PariAngle; //for Angle
	public float binScaleRate, PointDisThr, badPariWeight, weightThr, lineAngleStep, lineDistStep, docScoreThr;//for Angle
	public int sameLinePointNumThr; //for Angle
	public HistMultiD_Sparse_equalSizeBin_forFloat<Integer> hist_check; //for hist_check
	public MatchingInfo matchingInfo;
		
	public ScoreDoc(Disp disp, Conf_ImageR conf_ImageR) throws IOException, InterruptedException{
		ImageIndexInfo indexInfo=new ImageIndexInfo(conf_ImageR);
		ini(disp, conf_ImageR.sd_iniR_scheme, conf_ImageR.sd_rerankFlag, conf_ImageR.sd_rerankHEPara, indexInfo, false);
	}
	
	public ScoreDoc(Disp disp, Conf_ImageR conf_ImageR, ImageIndexInfo indexInfo) throws IOException, InterruptedException{
		ini(disp, conf_ImageR.sd_iniR_scheme, conf_ImageR.sd_rerankFlag, conf_ImageR.sd_rerankHEPara, indexInfo, false);
	}
	
	public ScoreDoc(Disp disp, String iniR_scheme, String scoreFlag, String rerankHEPara, Configuration conf, String docInfoPath, String TVectorInfoPath, boolean timeTest) throws IOException, InterruptedException{
		ImageIndexInfo indexInfo=new ImageIndexInfo(iniR_scheme, conf, docInfoPath, TVectorInfoPath); 
		ini(disp, iniR_scheme, scoreFlag, rerankHEPara, indexInfo, timeTest);
	}

	public ScoreDoc(Disp disp, String iniR_scheme, String scoreFlag, String rerankHEPara, ImageIndexInfo index_docIDInS, boolean timeTest) throws IOException, InterruptedException{
		ini(disp, iniR_scheme, scoreFlag, rerankHEPara, index_docIDInS, timeTest);
	}
	
	public void ini(Disp disp, String iniR_scheme, String scoreFlag, String rerankHEPara, ImageIndexInfo index_docIDInS, boolean timeTest) throws IOException, InterruptedException {
		StringBuffer returnInfo=new StringBuffer();
		boolean isOneVoteForTwoBins=true;
		//set iniR_weight
		returnInfo.append("iniR_scheme for initial retrieval step:"+iniR_scheme); 
		String[] infos=iniR_scheme.split("@");
		String iniR_label=infos[0];
		iniR_noBurst=false; iniR_BurstIntra=false; iniR_BurstInter=false; iniR_BurstIntraInter=false; iniR_1vw1match=false;  iniR_ASMK=false;
		if (iniR_label.equals("_iniR-noBurst")) {
			iniR_noBurst=true;
			HEPara_iniR=new HEParameters(Integer.valueOf(infos[1]), Double.valueOf(infos[2]));
			returnInfo.append(", "+HEPara_iniR);
		}else if (iniR_label.equals("_iniR-BurstIntra")) {
			iniR_BurstIntra=true;
			HEPara_iniR=new HEParameters(Integer.valueOf(infos[1]), Double.valueOf(infos[2]));
			returnInfo.append(", "+HEPara_iniR);
		}else if (iniR_label.equals("_iniR-BurstInter")) {
			iniR_BurstInter=true;
			HEPara_iniR=new HEParameters(Integer.valueOf(infos[1]), Double.valueOf(infos[2]));
			returnInfo.append(", "+HEPara_iniR);
		}else if (iniR_label.equals("_iniR-BurstIntraInter")) {
			iniR_BurstIntraInter=true;
			HEPara_iniR=new HEParameters(Integer.valueOf(infos[1]), Double.valueOf(infos[2]));
			returnInfo.append(", "+HEPara_iniR);
		}else if (iniR_label.equals("_iniR-1vw1match")) {
			iniR_1vw1match=true;
			HEPara_iniR=new HEParameters(Integer.valueOf(infos[1]), Double.valueOf(infos[2]));
			returnInfo.append(", "+HEPara_iniR);
		}else if (iniR_label.equals("_iniR-ASMK")) {//_iniR-ASMK@0@3
			iniR_ASMK=true;
			//set ASMK_thr, ASMK_alpa
			this.ASMK_thr=Double.valueOf(infos[1]);
			this.ASMK_alpa=Double.valueOf(infos[2]);
			returnInfo.append(", ASMK_thr:"+ASMK_thr+", ASMK_alpa:"+ASMK_alpa);
		}else {
			throw new InterruptedException("err! iniR_label should be:, _iniR-noBurst, _iniR-BurstIntra, _iniR-BurstIntra, _iniR-BurstIntraInter, _iniR_1vw1match or _iniR_ASMK, here:"+iniR_label);
		}
		//set scoreFlag
		returnInfo.append(", scoreFlag:"+scoreFlag);  //"_OriHE", "_1vs1", "_1vs1AndHPM", "_1vs1AndAngle"
		String[] scoreFlagParas=scoreFlag.split("@");
		General.Assert(scoreFlagParas[0].equalsIgnoreCase("_OriHE")||scoreFlagParas[0].equalsIgnoreCase("_1vs1")
				||scoreFlagParas[0].equalsIgnoreCase("_1vs1AndHPM")||scoreFlagParas[0].equalsIgnoreCase("_1vs1AndHist")||scoreFlagParas[0].equalsIgnoreCase("_HistAnd1vs1")
				||scoreFlagParas[0].equalsIgnoreCase("_1vs1AndAngle")||scoreFlagParas[0].equalsIgnoreCase("_1vs1AndHistAndAngle")||scoreFlagParas[0].equalsIgnoreCase("_HistAnd1vs1AndAngle")||scoreFlagParas[0].equalsIgnoreCase("_1vs1AndHistAndAngleWithHPM")
				, "err in scoreFlag: "+scoreFlag);
		this.scoreFlag=scoreFlag;
		//***** read doc info, TVector idf_squre ***//
		indexInfo=index_docIDInS;
		returnInfo.append("\n"+indexInfo.getIndexStat());
		//***** set HEPara_reR***//
		if (rerankHEPara!=null) {
			this.HEPara_reR=new HEParameters(rerankHEPara); //rerankHEPara: reRHE@18@12
			returnInfo.append("\n"+"HEPara_reR: "+HEPara_reR);
		}
		//***** set for 1vs1 ***//
		int i=1;
		if (scoreFlagParas[0].equalsIgnoreCase("_1vs1")) {//_1vs1@true@true
			this.isOnlyUseHMDistFor1Vs1=Boolean.valueOf(scoreFlagParas[i++]);
			this.is1vw1match=Boolean.valueOf(scoreFlagParas[i++]);
		}
		//***** set for HPM ***//
		if (scoreFlagParas[0].equalsIgnoreCase("_1vs1AndHPM")) {//_1vs1AndHPM@true@true@4@6
			this.isOnlyUseHMDistFor1Vs1=Boolean.valueOf(scoreFlagParas[i++]);
			this.is1vw1match=Boolean.valueOf(scoreFlagParas[i++]);
			this.HPM_ParaDim=Integer.valueOf(scoreFlagParas[i++]); 
			this.HPM_level=Integer.valueOf(scoreFlagParas[i++]); 
			scalingInfo_min_max=new double[][]{{-Math.PI,Math.PI},{Math.log10(0.1),Math.log10(10)},{0,0},{0,0}};//angle, scale, x, y
			returnInfo.append("\n"+"isOnlyUseHMDistFor1Vs1:"+isOnlyUseHMDistFor1Vs1+", for spatial HPM checking, HPM_ParaDim:"+HPM_ParaDim+", HPM_level:"+HPM_level);
		}
		//***** set for histCheck ***//
		if (scoreFlagParas[0].equalsIgnoreCase("_1vs1AndHist") || scoreFlagParas[0].equalsIgnoreCase("_HistAnd1vs1")) {//_1vs1AndHist@true@true@0.52@0.2
			this.isOnlyUseHMDistFor1Vs1=Boolean.valueOf(scoreFlagParas[i++]);
			this.is1vw1match=Boolean.valueOf(scoreFlagParas[i++]);
			float rotationStep=Float.valueOf(scoreFlagParas[i++]);
			float scaleStep=Float.valueOf(scoreFlagParas[i++]);
			float[][] begin_end_step=new float[][]{{(float) -Math.PI,(float) Math.PI,rotationStep},{-1,1,scaleStep}};
			this.hist_check=new HistMultiD_Sparse_equalSizeBin_forFloat<Integer>(true, isOneVoteForTwoBins);
			returnInfo.append("\n"+"isOnlyUseHMDistFor1Vs1:"+isOnlyUseHMDistFor1Vs1+", for spatial hist checking:");
			hist_check.makeEqualBins(begin_end_step, "0.00", returnInfo);//bins for orientation,scale,translation of matching links, -(float) (Math.PI), (float) (Math.PI)
		}
		//***** set for _1vs1AndAngle ***//
		if (scoreFlagParas[0].equalsIgnoreCase("_1vs1AndAngle")) {//_1vs1AndAngle@true@true@0.52@0.2@0.1@0.1@2@0.52@0.1@10
			this.isOnlyUseHMDistFor1Vs1=Boolean.valueOf(scoreFlagParas[i++]);
			this.is1vw1match=Boolean.valueOf(scoreFlagParas[i++]);
			float rotationStep=Float.valueOf(scoreFlagParas[i++]);
			float scaleStep=Float.valueOf(scoreFlagParas[i++]);
			float[][] begin_end_step=new float[][]{{(float) -Math.PI,(float) Math.PI,rotationStep},{-1,1,scaleStep}};
			this.binScaleRate=Float.valueOf(scoreFlagParas[i++]); 
			this.PointDisThr=Float.valueOf(scoreFlagParas[i++]); 
			this.badPariWeight=Float.valueOf(scoreFlagParas[i++]); 
			this.weightThr=Float.valueOf(scoreFlagParas[i++]); 
			this.lineAngleStep=Float.valueOf(scoreFlagParas[i++]); 
			this.lineDistStep=Float.valueOf(scoreFlagParas[i++]); 
			this.sameLinePointNumThr=Integer.valueOf(scoreFlagParas[i++]);
			this.docScoreThr=Float.valueOf(scoreFlagParas[i++]); 
			this.hist_PariAngle=new HistMultiD_Sparse_equalSizeBin_forFloat<Pair_int>(true,isOneVoteForTwoBins);
			returnInfo.append("\n"+"isOnlyUseHMDistFor1Vs1:"+isOnlyUseHMDistFor1Vs1+", for spatial Angle checking, rotationStep:"+rotationStep+", scaleStep:"+scaleStep
					+", PointDisThr:"+PointDisThr+", badPariWeight:"+badPariWeight+", weightThr:"+weightThr);
			hist_PariAngle.makeEqualBins(begin_end_step, "0.00", returnInfo);//bins for angle of two matching links			
		}
		//***** set for _1vs1AndHistAndAngle ***//
		if (scoreFlagParas[0].equalsIgnoreCase("_1vs1AndHistAndAngle") || scoreFlagParas[0].equalsIgnoreCase("_HistAnd1vs1AndAngle")) {//_1vs1AndHistAndAngle@true@true@false@0.52@0.2@0.1@0.1@2@0.52@0.1@10
			this.isOnlyUseHMDistFor1Vs1=Boolean.valueOf(scoreFlagParas[i++]);
			this.is1vw1match=Boolean.valueOf(scoreFlagParas[i++]);
			this.isUpRightFeat=Boolean.valueOf(scoreFlagParas[i++]);
			float rotationStep=Float.valueOf(scoreFlagParas[i++]);
			float scaleStep=Float.valueOf(scoreFlagParas[i++]);
			float[][] begin_end_step=new float[][]{{(float) -Math.PI,(float) Math.PI,rotationStep},{-1,1,scaleStep}};
			this.binScaleRate=Float.valueOf(scoreFlagParas[i++]); 
			this.PointDisThr=Float.valueOf(scoreFlagParas[i++]); 
			this.badPariWeight=Float.valueOf(scoreFlagParas[i++]); 
			this.weightThr=Float.valueOf(scoreFlagParas[i++]); 
			this.lineAngleStep=Float.valueOf(scoreFlagParas[i++]); 
			this.lineDistStep=Float.valueOf(scoreFlagParas[i++]); 
			this.sameLinePointNumThr=Integer.valueOf(scoreFlagParas[i++]);
			this.docScoreThr=Float.valueOf(scoreFlagParas[i++]); 
			this.hist_check=new HistMultiD_Sparse_equalSizeBin_forFloat<Integer>(true, isOneVoteForTwoBins);
			returnInfo.append("\n"+"isOnlyUseHMDistFor1Vs1:"+isOnlyUseHMDistFor1Vs1+", for spatial hist checking:");
			hist_check.makeEqualBins(begin_end_step, "0.00", returnInfo);//bins for orientation,scale,translation of matching links, -(float) (Math.PI), (float) (Math.PI)
		}
		//***** set for _1vs1AndHistAndAngleWithHPM ***//
		if (scoreFlagParas[0].equalsIgnoreCase("_1vs1AndHistAndAngleWithHPM")) {//_1vs1AndHistAndAngleWithHPM@true@true@false@0.52@0.2@0.1@0.1@2@0.52@0.1@10@2@6
			this.isOnlyUseHMDistFor1Vs1=Boolean.valueOf(scoreFlagParas[1]);
			this.is1vw1match=Boolean.valueOf(scoreFlagParas[i++]);
			this.isUpRightFeat=Boolean.valueOf(scoreFlagParas[i++]);
			float rotationStep=Float.valueOf(scoreFlagParas[i++]);
			float scaleStep=Float.valueOf(scoreFlagParas[i++]);
			float[][] begin_end_step=new float[][]{{(float) -Math.PI,(float) Math.PI,rotationStep},{-1,1,scaleStep}};
			this.binScaleRate=Float.valueOf(scoreFlagParas[i++]); 
			this.PointDisThr=Float.valueOf(scoreFlagParas[i++]); 
			this.badPariWeight=Float.valueOf(scoreFlagParas[i++]); 
			this.weightThr=Float.valueOf(scoreFlagParas[i++]); 
			this.lineAngleStep=Float.valueOf(scoreFlagParas[i++]); 
			this.lineDistStep=Float.valueOf(scoreFlagParas[i++]); 
			this.sameLinePointNumThr=Integer.valueOf(scoreFlagParas[i++]);
			this.docScoreThr=Float.valueOf(scoreFlagParas[i++]); 
			this.HPM_ParaDim=Integer.valueOf(scoreFlagParas[i++]); 
			this.HPM_level=Integer.valueOf(scoreFlagParas[i++]); 
			scalingInfo_min_max=new double[][]{{-Math.PI,Math.PI},{Math.log10(0.1),Math.log10(10)},{0,0},{0,0}};//angle, scale, x, y
			this.hist_check=new HistMultiD_Sparse_equalSizeBin_forFloat<Integer>(true, isOneVoteForTwoBins);
			returnInfo.append("\n"+"isOnlyUseHMDistFor1Vs1:"+isOnlyUseHMDistFor1Vs1+", for spatial hist checking:");
			hist_check.makeEqualBins(begin_end_step, "0.00", returnInfo);//bins for orientation,scale,translation of matching links, -(float) (Math.PI), (float) (Math.PI)
			returnInfo.append(", for spatial HPM checking if Angle check failed, HPM_ParaDim:"+HPM_ParaDim+", HPM_level:"+HPM_level);
		}
		//******** setup for calculating time ********
		if (timeTest) {
			matchingInfo=new MatchingInfo();
		}
		//******** setup finished ********
		disp.disp("setup_scoreDoc finished! parameters: "+returnInfo.toString());
	}
	
	public <T extends I_HESig & FeatInd> ArrayList<DID_Score> scoreDocs_inOneTVector(T[] queryFeats, byte[] queryAggSig, int vw, TVector tVector) throws InterruptedException{
		//********* search one TVector for one query  *********
		ArrayList<DID_Score> docs_scores=new ArrayList<DID_Score>(2*tVector.docNum()); 
		if (iniR_noBurst) {
			//compare docs in TVector for this query with only HE weighting, no Burst Weighting
			for(Int_SURFfeat_ShortArr docID_feats : tVector.docID_feats){
				//get match link and score
				float hmScore=General_BoofCV.compare_HESigs(queryFeats, docID_feats.feats.feats, HEPara_iniR.HMDistThr, HEPara_iniR.hammingW, null);
				if (hmScore>0) {
					docs_scores.add(new DID_Score(docID_feats.integer, hmScore*indexInfo.idf_squre[vw]));
				}
			}
		}else if (iniR_1vw1match) {
			for(Int_SURFfeat_ShortArr docID_feats : tVector.docID_feats){
				//get match link and score
				LinkedList<MatchFeat> thisDocMatches=new LinkedList<MatchFeat>();
				float hmScore=General_BoofCV.compare_HESigs(queryFeats, docID_feats.feats.feats, HEPara_iniR.HMDistThr, HEPara_iniR.hammingW, thisDocMatches);
				if (hmScore>0) {//this doc exist matches
					//find best match
					int minDist=Integer.MAX_VALUE; MatchFeat bestMatch=null;
					for (MatchFeat matchFeat : thisDocMatches) {
						if (minDist>matchFeat.HMDist) {
							minDist=matchFeat.HMDist;
							bestMatch=matchFeat;
						}
					}
					//only use 1 bestMatch for 1 vw
					docs_scores.add(new DID_Score(docID_feats.integer, HEPara_iniR.hammingW[bestMatch.HMDist]*indexInfo.idf_squre[vw]));
				}
			}
		}else if (iniR_ASMK) {
			if (ASMK_weight==null) {//ASMK_weight does not exist, this is the fist time for scoreDocs_inOneTVector 
				/**
				 * 2015_IJCV_Image Search with Selective Match Kernels: Aggregation Across Single and Multiple Images
				 * e.7: 2h(a, b) = B(1 − <a, b>h)
				 * HESim is [1,-2/B,-1] cossponding to HEDist 0~B, and it is >0 if hammingDist < B/2;
				 */
				ASMK_weight=General.makeRange(new double[]{1,-1,-2d/(queryAggSig.length*8)});
				for (int i = 0; i < ASMK_weight.length; i++) {
					ASMK_weight[i]=Math.signum(ASMK_weight[i])*Math.pow(Math.abs(ASMK_weight[i]), ASMK_alpa);
				}
				ASMK_thr=Math.signum(ASMK_thr)*Math.pow(Math.abs(ASMK_thr), ASMK_alpa);
			}
			General.Assert(ASMK_weight.length==(queryAggSig.length*8+1), "err! ASMK_weight.length should ==(queryAggSig.length*8+1), here: ASMK_weight.length:"+ASMK_weight.length+", queryAggSig.length:"+queryAggSig.length);
			for(Int_SURFfeat_ShortArr docID_feats : tVector.docID_feats){
				double HESim=General_BoofCV.computeHESim_fromHESig(queryAggSig, docID_feats.feats.aggSig, ASMK_weight);
				if (HESim>ASMK_thr) {//this doc is good
					docs_scores.add(new DID_Score(docID_feats.integer, (float) (HESim*indexInfo.idf_squre[vw])));
				}
			}
		}else if (iniR_BurstInter || iniR_BurstIntra || iniR_BurstIntraInter) {
			//get MatchFeat: short HMDist, int QFeatInd, SURFfeat_noSig docFeat;
			int start_HESig=0; int end_HESig=queryFeats[0].getHESig().length;
			float[] AllQFeatScore_eachDocs=new float[tVector.docNum()];
	    	for(int qi=0;qi<queryFeats.length;qi++){
	    		//process matches for one query's descriptor 
	    		float matchScore_allDoc=0; 
	    		float[] matchScore_eachDoc=new float[tVector.docNum()]; 
	    		float[] matchScoreSquare_eachDocs=new float[tVector.docNum()];
    			int doc_i = 0;
    			for (Int_SURFfeat_ShortArr docID_feats : tVector.docID_feats) {
	    			//process one doc
	    			SURFfeat[] feats_doc=docID_feats.feats.feats;
	    			for(int dj=0;dj<feats_doc.length;dj++){
						int hammingDist=General.get_DiffBitNum(queryFeats[qi].getHESig(), feats_doc[dj].getHESig(), start_HESig, end_HESig);// computing time: 15% of BigInteger!!
						if(hammingDist<=HEPara_iniR.HMDistThr){
							float hmScore=HEPara_iniR.hammingW[hammingDist];
							matchScore_allDoc+=hmScore;
							matchScore_eachDoc[doc_i]+=hmScore;
							matchScoreSquare_eachDocs[doc_i]+=iniR_BurstIntraInter?Math.pow(hmScore, 2):Math.pow(hmScore, 1.5);
						}
					}
	    			doc_i++;
				}
	    		//apply bust weighting
	    		matchScore_allDoc=(float) (1/Math.sqrt(matchScore_allDoc));// 1/(t_q)^(0.5)
	    		for (doc_i = 0; doc_i < matchScoreSquare_eachDocs.length; doc_i++) {
					if (matchScoreSquare_eachDocs[doc_i]>0) {//this doc has matches
						if (iniR_BurstIntra) {
							AllQFeatScore_eachDocs[doc_i]+=matchScoreSquare_eachDocs[doc_i]*(1/Math.sqrt(matchScore_eachDoc[doc_i]));
						}else if (iniR_BurstInter) {
							AllQFeatScore_eachDocs[doc_i]+=matchScoreSquare_eachDocs[doc_i]*matchScore_allDoc;
						}else {
							AllQFeatScore_eachDocs[doc_i]+=matchScoreSquare_eachDocs[doc_i]*(1/Math.sqrt(matchScore_eachDoc[doc_i]))*matchScore_allDoc;
						}
					}
				}
			}
	    	//make result
	    	int doc_i = 0;
	    	for (Int_SURFfeat_ShortArr one : tVector.docID_feats) {//docs_scores and matches are the two result, both are in the same order of docs.
	    		if (AllQFeatScore_eachDocs[doc_i]>0) {
	    			docs_scores.add(new DID_Score(one.integer, AllQFeatScore_eachDocs[doc_i]));
				}
	    		doc_i++;
			}
		}
		return docs_scores;
	}
	
	public LinkedList<Int_MatchFeatArr> collectMatches_inOneTVector(int[] topDocs_inSortedDocIDs, SURFfeat_ShortArr_AggSig queryFeats, int[] docIDs_inTVector, TVector tVector) throws InterruptedException{
		LinkedList<Int_SURFfeat_ShortArr> selectedDocs=SelectID.select_in_twoSorted_ASC(topDocs_inSortedDocIDs, tVector.docID_feats);
		LinkedList<Int_MatchFeatArr> docID_matches=new LinkedList<>();
		if (selectedDocs.size()>0) {
			for (Int_SURFfeat_ShortArr oneDoc : selectedDocs) {
				//get match link and score
				SURFfeat_ShortArr_AggSig thisDocFeats = oneDoc.feats;
				LinkedList<MatchFeat> thisDocMatches=new LinkedList<MatchFeat>();
//				if (iniR_ASMK) {//this is not good, do not use asmk to pre-filter vw
//					double HESim=General_BoofCV.computeHESim_fromHESig(queryFeats.aggSig, thisDocFeats.aggSig, ASMK_weight);
//					if (HESim>ASMK_thr) {//this doc is good
//						float hmScore=General_BoofCV.compare_HESigs(queryFeats.feats, thisDocFeats.feats, HEPara_reR.HMDistThr, HEPara_reR.hammingW, thisDocMatches);
//						if (hmScore>0) {//this doc exist matches
//							docID_matches.add(new Int_MatchFeatArr(oneDoc.integer, new MatchFeat_Arr(thisDocMatches)));
//						}
//					}
//				}else{
					float hmScore=General_BoofCV.compare_HESigs(queryFeats.feats, thisDocFeats.feats, HEPara_reR.HMDistThr, HEPara_reR.hammingW, thisDocMatches);
					if (hmScore>0) {//this doc exist matches
						docID_matches.add(new Int_MatchFeatArr(oneDoc.integer, new MatchFeat_Arr(thisDocMatches)));
					}
//				}
			}
		}
		if (docID_matches.size()>0) {
			return docID_matches;
		}else {
			return null;
		}
		
//		ArrayList<int[]> commons=General.findCommonElementInds_twoSorted_ASC_loopShotArr(topDocs_inSortedDocIDs, docIDs_inTVector);//find common elements in two sorted arr
//		LinkedList<Int_MatchFeatArr> docID_matches=new LinkedList<>();
//		if (commons!=null) {
//			for (int[] docID_index : commons) {//docID_indexIntopDocsinSortedDocIDs_indexIndocIDs_inTVector
//				//get match link and score
//				SURFfeat_ShortArr_AggSig thisDocFeats = tVector.docID_feats.get(docID_index[2]).feats;
//				LinkedList<MatchFeat> thisDocMatches=new LinkedList<MatchFeat>();
////				if (iniR_ASMK) {
////					double HESim=General_BoofCV.computeHESim_fromHESig(queryFeats.aggSig, thisDocFeats.aggSig);
////					if (HESim>ASMK_thr) {//this doc is good
////						float hmScore=General_BoofCV.compare_HESigs(queryFeats.feats, thisDocFeats.feats, HEPara_reR.HMDistThr, HEPara_reR.hammingW, thisDocMatches);
////						if (hmScore>0) {//this doc exist matches
////							docID_matches.add(new Int_MatchFeatArr(docID_index[0], new MatchFeat_Arr(thisDocMatches)));
////						}
////					}
////				}else{
//					float hmScore=General_BoofCV.compare_HESigs(queryFeats.feats, thisDocFeats.feats, HEPara_reR.HMDistThr, HEPara_reR.hammingW, thisDocMatches);
//					if (hmScore>0) {//this doc exist matches
//						docID_matches.add(new Int_MatchFeatArr(docID_index[0], new MatchFeat_Arr(thisDocMatches)));
//					}
////				}
//			}
//		}
//		if (docID_matches.size()>0) {
//			return docID_matches;
//		}else {
//			return null;
//		}
	}
	
 	public float[] scoreOneDoc(PhotoAllFeats_orgVW queryFeat, PhotoAllFeats_orgVW docFeat, 
			LinkedList<ImageRegionMatch> finalMatches, LinkedList<SURFpoint> selPoints_l, LinkedList<SURFpoint> selPoints_r, 
			boolean disp) throws InterruptedException, IOException{
		//********* find initial matches  **********		
		ArrayList<Int_MatchFeatArr>  matches = findIniMatches(queryFeat, docFeat);
		//**************** score this doc  ****************
		float[] docScores=new float[2];
		if (matches.size()==0) {
//			System.out.println("Q:"+queryFeat.ID+", no initial matching for doc:"+docFeat.ID);
		}else {
			DocAllMatchFeats allMatches=new DocAllMatchFeats(docFeat.ID, matches.toArray(new Int_MatchFeatArr[0]));
			docScores=scoreOneDoc(allMatches, queryFeat.getIntersetPoint(), queryFeat.ID, queryFeat.getMaxDim(), finalMatches, selPoints_l, selPoints_r, disp);
		}
		return docScores;		
	}
	
	public ArrayList<Int_MatchFeatArr>  findIniMatches(PhotoAllFeats_orgVW queryFeat, PhotoAllFeats_orgVW docFeat) throws InterruptedException{
		//********* find initial matches  **********		
		ArrayList<Int_MatchFeatArr>  matches = new ArrayList<Int_MatchFeatArr>(); 
		for (Entry<Integer, SURFfeat_ShortArr_AggSig> one_VW_Sigs : queryFeat.VW_Sigs.entrySet()) {//loop over query's all vws
			int vw=one_VW_Sigs.getKey();
			SURFfeat[] sigs_Query=one_VW_Sigs.getValue().feats;
			SURFfeat_ShortArr_AggSig sigs_Doc=docFeat.VW_Sigs.get(vw);
			if(sigs_Doc!=null){//doc exist this vw
				//********* get match link and score ********
				ArrayList<MatchFeat> matchFeats=General_BoofCV.compare_HESigs(sigs_Query, sigs_Doc.feats, HEPara_reR.HMDistThr);
				if (matchFeats!=null) {
					matches.add(new Int_MatchFeatArr(vw,new MatchFeat_Arr(matchFeats)));
				}
			}
		}
		return matches;
	}
	
	public float[] scoreOneDoc(DocAllMatchFeats docsMatches, SURFpoint[] thisQueryFeats, int queryID, int query_MaxDim, 
			LinkedList<ImageRegionMatch> showMatches, LinkedList<SURFpoint> selPoints_l, LinkedList<SURFpoint> selPoints_r, 
			boolean disp) throws InterruptedException, IOException{
		float[] res=null; String coreFlag=scoreFlag.split("@")[0];
		if (coreFlag.equalsIgnoreCase("_OriHE")) {
			res=new float[]{General_BoofCV.scoreDoc_byOriHMScore(docsMatches, queryID, showMatches, indexInfo.docNorms, HEPara_reR.hammingW, indexInfo.idf_squre, disp),0};
		}else if (coreFlag.equalsIgnoreCase("_1vs1")) {
			res=  new float[]{General_BoofCV.scoreDoc_by1vs1(docsMatches, queryID, showMatches, indexInfo.docNorms, HEPara_reR.hammingW, indexInfo.idf_squre, isOnlyUseHMDistFor1Vs1, is1vw1match, matchingInfo, disp),0};
		}else if (coreFlag.equalsIgnoreCase("_1vs1AndHPM")) {
			res=new float[]{General_BoofCV.scoreDoc_by1vs1AndHPM(docsMatches, thisQueryFeats, queryID, showMatches,
					indexInfo.docNorms, HEPara_reR.hammingW, indexInfo.idf_squre, isOnlyUseHMDistFor1Vs1, is1vw1match, indexInfo.doc_maxDim, scalingInfo_min_max, HPM_ParaDim, HPM_level, 
					disp),0};
		}else if (coreFlag.equalsIgnoreCase("_1vs1AndHist")) {
			res=new float[]{General_BoofCV.scoreDoc_by1vs1AndHist(true, docsMatches, thisQueryFeats, queryID, showMatches,
					indexInfo.docNorms, HEPara_reR.hammingW, indexInfo.idf_squre, isOnlyUseHMDistFor1Vs1, is1vw1match, hist_check, 
					disp),0};
		}else if (coreFlag.equalsIgnoreCase("_HistAnd1vs1")) {
			res=new float[]{General_BoofCV.scoreDoc_by1vs1AndHist(false, docsMatches, thisQueryFeats, queryID, showMatches,
					indexInfo.docNorms, HEPara_reR.hammingW, indexInfo.idf_squre, isOnlyUseHMDistFor1Vs1, is1vw1match, hist_check, 
					disp),0};
		}else if (coreFlag.equalsIgnoreCase("_1vs1AndAngle")) {
			res=General_BoofCV.scoreDoc_by1vs1AndAngle(docsMatches, thisQueryFeats, queryID, query_MaxDim, showMatches,
					indexInfo.docNorms, HEPara_reR.hammingW, indexInfo.idf_squre, isOnlyUseHMDistFor1Vs1, is1vw1match, indexInfo.doc_maxDim, hist_PariAngle, 
					PointDisThr, badPariWeight, weightThr, lineAngleStep, lineDistStep, sameLinePointNumThr, docScoreThr, disp);
		}else if (coreFlag.equalsIgnoreCase("_1vs1AndHistAndAngle")) {
//			res=General_BoofCV.scoreDoc_by1vs1AndHistAndAngle(true, docsMatches, thisQueryFeats, isUpRightFeat, queryID, query_MaxDim, showMatches,
//					indexInfo.docNorms, HEPara_reR.hammingW, indexInfo.idf_squre, isOnlyUseHMDistFor1Vs1, is1vw1match, indexInfo.doc_maxDim, hist_check, binScaleRate, 
//					false, null, 0, 0,
//					PointDisThr, badPariWeight, weightThr, lineAngleStep, lineDistStep, sameLinePointNumThr, docScoreThr, 
//					matchingInfo, disp, 
//					selPoints_l, selPoints_r);
			//for test PGMSource
			float score1=PGMSource.scoreDoc_byPGM(docsMatches, thisQueryFeats, queryID, isUpRightFeat, indexInfo.idf_squre, disp, hist_check, disp);
			res=new float[]{score1,0};
		}else if (coreFlag.equalsIgnoreCase("_HistAnd1vs1AndAngle")) {
			res=General_BoofCV.scoreDoc_by1vs1AndHistAndAngle(false, docsMatches, thisQueryFeats, isUpRightFeat, queryID, query_MaxDim, showMatches,
					indexInfo.docNorms, HEPara_reR.hammingW, indexInfo.idf_squre, isOnlyUseHMDistFor1Vs1, is1vw1match, indexInfo.doc_maxDim, hist_check, binScaleRate, 
					false, null, 0, 0,
					PointDisThr, badPariWeight, weightThr, lineAngleStep, lineDistStep, sameLinePointNumThr, docScoreThr, 
					matchingInfo, disp, 
					selPoints_l, selPoints_r);
		}else if (coreFlag.equalsIgnoreCase("_1vs1AndHistAndAngleWithHPM")) {
			res=General_BoofCV.scoreDoc_by1vs1AndHistAndAngle(true, docsMatches, thisQueryFeats, isUpRightFeat, queryID, query_MaxDim, showMatches,
					indexInfo.docNorms, HEPara_reR.hammingW, indexInfo.idf_squre, isOnlyUseHMDistFor1Vs1, is1vw1match, indexInfo.doc_maxDim, hist_check, binScaleRate, 
					true, scalingInfo_min_max, HPM_ParaDim, HPM_level,
					PointDisThr, badPariWeight, weightThr, lineAngleStep, lineDistStep, sameLinePointNumThr, docScoreThr, 
					matchingInfo, disp, 
					selPoints_l, selPoints_r);
		}else {
			throw new InterruptedException("err in scoreOneDoc, scoreFlag should be _OriHE, _1vs1, _1vs1AndHPM, _1vs1AndAngle, here scoreFlag:"+scoreFlag);
		}
		if (matchingInfo!=null) {
			matchingInfo.scoreDocNum++; 
		}
		return res;
	}

}
