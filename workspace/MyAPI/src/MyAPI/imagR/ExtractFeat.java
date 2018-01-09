package MyAPI.imagR;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.ejml.data.DenseMatrix64F;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.General_Hadoop;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.PhotoAllFeats;
import MyCustomedHaoop.ValueClass.SURFfeat;
import MyCustomedHaoop.ValueClass.SURFpoint;

public class ExtractFeat {
	
	public static class FeatState{
		public int point_num;
		public int vw_num;
		public int mutiAss_num; 
		public int uniqueVW_num;
		
		public FeatState(int point_num, int vw_num, int mutiAss_num, int uniqueVW_num){
			this.point_num=point_num;
			this.vw_num=vw_num;
			this.mutiAss_num=mutiAss_num;
			this.uniqueVW_num=uniqueVW_num;
		}
		
		@Override
		public String toString(){
			return "point_num:"+point_num+", vw_num:"+vw_num+", averaged mutiAss_num:"+mutiAss_num+", uniqueVW_num:"+uniqueVW_num;
		}
	}
	
	//config for feature extraction
	public ExtractRawFeat extRawFeat;
	public float[][] centers;
	public DenseMatrix64F pMatrix;
	public float[][] HEThreshold;
	public ArrayList<HashSet<Integer>> node_vw_links;
	public float[][] middleNodes;
	public int vws_NN;
	public double alph_NNDist;
	public double deta_vwSoftWeight;
	//rawFeat statics
	private int totPhoNum;
	private int noFeatPhoNum;
	private int totFeatNum;
	private int maxFeatNum;

	public ExtractFeat(Disp disp, Conf_ImageR confImageR) throws IOException, InterruptedException{
		this(disp, confImageR.ef_targetFeat,"./"+Conf_ImageR.ef_detector_localPoint, Conf_ImageR.ef_tempFilesPath, 
				Conf_ImageR.ef_vwCenters, Conf_ImageR.ef_PMatrix, Conf_ImageR.ef_HE_Thresholds, Conf_ImageR.ef_middleNodes, 
				Conf_ImageR.ef_node_vw_links, confImageR.ef_mutiAssVW, confImageR.conf);
	}
	
	@SuppressWarnings("unchecked")
	public ExtractFeat(Disp disp, String targetFeat, String binaryPath_Detector, String tempFilesPath, String vwCenters_path, String PMatrix_path, String HE_Thresholds_path, String middleNodes_path, 
			String node_vw_links_path, MutiAssVW mutiAssVW, Configuration conf) throws IOException, InterruptedException {
		StringBuffer returnInfo=new StringBuffer();
		//***************** set targetFeat ***//
		extRawFeat=new ExtractRawFeat(targetFeat, binaryPath_Detector, tempFilesPath);
		returnInfo.append(", "+extRawFeat.configInfo);
		//***************** read visual word cluster centers***//
		centers=General_Hadoop.readMatrixFromSeq_floatMatrix(conf, General_Hadoop.getLocalPath(vwCenters_path, conf)); //vwCenters_path can be null
		returnInfo.append(centers==null?", no visual word Centers loaded":", visual word number: "+centers.length);
		//***************** read pMatrix ***//
		pMatrix=(DenseMatrix64F) General.readObject(PMatrix_path); //PMatrix_path can be null
		//***************** read HEThreshold ***//
		HEThreshold=General_Hadoop.readMatrixFromSeq_floatMatrix(conf, General_Hadoop.getLocalPath(HE_Thresholds_path, conf));//HE_Thresholds_path can be null
		returnInfo.append(HEThreshold==null?", no HEThreshold loaded":", HEThreshold number: "+HEThreshold.length+", HEThreshold-bitNum:"+HEThreshold[0].length);
		//***************** read middle-nodes
		middleNodes=General_Hadoop.readMatrixFromSeq_floatMatrix(conf, General_Hadoop.getLocalPath(middleNodes_path, conf));//middleNodes_path can be null
		returnInfo.append(middleNodes==null?", no middleNodes loaded":", middleNodes-number: "+middleNodes.length);
		//**************** load node_vw_links
		node_vw_links= (ArrayList<HashSet<Integer>>) General.readObject(node_vw_links_path);//node_vw_links_path can be null
		if (node_vw_links==null) {
			returnInfo.append(", no node_vw_links loaded");
		}else {
			int maxLength=0;  int minLength=999999; 
			for(int i=0;i<node_vw_links.size();i++){
				maxLength=Math.max(node_vw_links.get(i).size(),maxLength);
				minLength=Math.min(node_vw_links.get(i).size(),minLength);
			}
			returnInfo.append(", node_vw_links: link number per node, maxLength: "+maxLength+", minLength:"+minLength);
		}
		//set vws_NN
		this.vws_NN=mutiAssVW.vws_NN;//10
		//set alph_NNDist
		this.alph_NNDist=mutiAssVW.alph_NNDist;//1.2
		this.deta_vwSoftWeight=mutiAssVW.deta_vwSoftWeight; //as SURF feature dist with its vw usually is 0.1~0.14, so deta is set to 0.05~0.1 
		
		//******** setup finished ********
		disp.disp("setup_extractFeat finished! "+returnInfo
					+", vws_NN:"+vws_NN+", alph_NNDist:"+alph_NNDist+", deta_vwSoftWeight:"+deta_vwSoftWeight+", current memory:"+General.memoryInfo());
 	}
	
	public double[][] extractRawFeature(String imgMark, BufferedImage photoImg, ArrayList<SURFpoint> interestPoints, boolean disp) throws IOException, InterruptedException{
		double[][] photoFeat=extRawFeat.extractRawFeature(imgMark, photoImg, interestPoints, disp);
		totPhoNum++;
		if (photoFeat==null) {
			noFeatPhoNum++;
		}else{
			totFeatNum+=photoFeat.length;
			maxFeatNum=Math.max(maxFeatNum, photoFeat.length);
		}
		return photoFeat;
	}

	public PhotoAllFeats makePhotoAllFeats(int width, int height, double[][] queryFeat, SURFpoint[] interestPoints) throws IOException, InterruptedException {
		return General_BoofCV.makePhotoAllFeats(width, height, queryFeat, interestPoints, centers, pMatrix, 
				HEThreshold, node_vw_links, middleNodes, vws_NN, alph_NNDist, deta_vwSoftWeight);
	}
	
	public FeatState makeVW_HESig(HashMap<Integer,ArrayList<SURFfeat>> VW_Sigs, double[][] queryFeat, SURFpoint[] interestPoints) throws IOException, InterruptedException{
		PhotoAllFeats feat= makePhotoAllFeats(0, 0, queryFeat,interestPoints); 
		return General_BoofCV.group_VW_SURFfeat(VW_Sigs, feat.feats);//feat num, vw num, mutiAssNum, uniqueVW num
	}
	
	public FeatState extractRawFeat_makeVW_HESig(String imgMark, HashMap<Integer, ArrayList<SURFfeat>> VW_Sigs, ArrayList<SURFpoint> interestPoints, BufferedImage photoImg, boolean disp) throws IOException, InterruptedException{
		if (interestPoints==null) {
			interestPoints=new ArrayList<SURFpoint>();
		}
		double[][] photoFeat=extractRawFeature(imgMark, photoImg, interestPoints, disp);
		return makeVW_HESig(VW_Sigs, photoFeat, interestPoints.toArray(new SURFpoint[0]));//feat num, vw num, mutiAssNum, uniqueVW num
	}
	
	public PhotoAllFeats extractRawFeat_makePhotoAllFeats(String imgMark, BufferedImage photoImg, boolean disp) throws IOException, InterruptedException{
		ArrayList<SURFpoint> interestPoints=new ArrayList<SURFpoint>();
		double[][] photoFeat=extractRawFeature(imgMark, photoImg, interestPoints, disp);
		return makePhotoAllFeats(photoImg.getWidth(), photoImg.getHeight(), photoFeat, interestPoints.toArray(new SURFpoint[0]));
	}
	
	public void getRawFeatStatis(Disp disp){
		disp.disp("RawFeatStatis in ExtractFeat, totPhoNum:"+totPhoNum+", noFeatPhoNum:"+noFeatPhoNum+", totFeatNum:"+totFeatNum+", on average per pho:"+(float)totFeatNum/(totPhoNum-noFeatPhoNum)+", maxFeatNum:"+maxFeatNum);
	}

	public static void main(String[] args) {
		General.checkDir(new Disp(true, "", null),".");
	}

}
