package SURFVW;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;

import weka.core.Instances;
import MyAPI.General.General;
import MyAPI.General.General_Weka;

public class GANN_makeNodes {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		//********* generate middle cluster nodes **************//
		//read real visual word cluster centers
		String vw_path="O:/ImageRetrieval/SURFVW/SURFVW_20K_I90";
//		String vw_path="/tudelft.net/staff-bulk/ewi/mm/D-MIRLab/XinchaoLi/ICMR2013/SURFVW/SURFVW_20K_I90";
		BufferedReader intstr_data = new BufferedReader(new InputStreamReader(new FileInputStream(vw_path), "UTF-8"));
	    String line1Photo; int numVW=0; 
	    while((line1Photo=intstr_data.readLine())!=null){ //line1Photo: VL-99983{n=3401 c=[-34.177, 153.369, .....] r=[7.903, 14.364, .....]}
	    	numVW++;
		}intstr_data.close(); 
		double[][] centers=new double[numVW][];
		int lineindex=0;
		intstr_data = new BufferedReader(new InputStreamReader(new FileInputStream(vw_path), "UTF-8"));
		while((line1Photo=intstr_data.readLine())!=null){ //line1Photo: VL-99983{n=3401 c=[-34.177, 153.369, .....] r=[7.903, 14.364, .....]}
			centers[lineindex]=General.StrArrToDouArr(line1Photo.split("\\[")[1].split("\\]")[0].split(","));
			lineindex++;
		}intstr_data.close();
		System.out.println("real visual word numbers: "+numVW);
		//kmean by Weka
		int numMiddleNode=1000; int randomSeed=7; int maxInteration=200; 
		ArrayList<double[]> nodes=new ArrayList<double[]> (numMiddleNode);
		Instances vws=General_Weka.makeWekaInstsforClustering_from_Arr(centers,null,"vws");
		System.out.println("start kmean on vws to find middle nodes, maxInteration: "+maxInteration);
		General_Weka.KMeans(vws,numMiddleNode ,randomSeed , maxInteration ,nodes , null);
		//check
		System.out.println("middle node done! nodes number: "+nodes.size());
		General.Assert(numMiddleNode==nodes.size(), "numMiddleNode is not equal to nodes.size(), kmean may not generate enough clusters");
		//make initial links
		ArrayList<HashSet<Integer>> node_vw_links=new ArrayList<HashSet<Integer>>(numMiddleNode);
		for(int node=0;node<numMiddleNode;node++){
			HashSet<Integer> oneNodeLink=new HashSet<Integer>();
			node_vw_links.add(oneNodeLink);
		}
		long endTime, startTime;   
		startTime=System.currentTimeMillis(); //startTime
		for(int i=0;i<centers.length;i++){
			int label_1=General.assignFeatToCenter(centers[i], nodes);
			node_vw_links.get(label_1).add(i);
		}
		endTime=System.currentTimeMillis(); //end time 
		System.out.println( centers.length+" feats classify finished!! ......"+ General.dispTime (endTime-startTime, "s"));
		System.out.println("node_vw_links node done! node_vw_links: "+node_vw_links.size());
		int maxLength=0;  int minLength=999999; 
		for(int i=0;i<node_vw_links.size();i++){
			maxLength=Math.max(node_vw_links.get(i).size(),maxLength);
			minLength=Math.min(node_vw_links.get(i).size(),minLength);
		}
		System.out.println("node_vw_links, link number per node, maxLength: "+maxLength+", minLength:"+minLength);
		//save nodes, node_vw_links
		General.writeObject("/tudelft.net/staff-bulk/ewi/mm/D-MIRLab/XinchaoLi/ICMR2013/SURFVW/middleNodes_M"+numMiddleNode+"_VW"+numVW+"_I"+maxInteration+".ArrayList_HashSet", nodes);
		General.writeObject("/tudelft.net/staff-bulk/ewi/mm/D-MIRLab/XinchaoLi/ICMR2013/SURFVW/node_vw_links_M"+numMiddleNode+"_VW"+numVW+"_I"+maxInteration+".ArrayList_HashSet", node_vw_links);


	}

}
