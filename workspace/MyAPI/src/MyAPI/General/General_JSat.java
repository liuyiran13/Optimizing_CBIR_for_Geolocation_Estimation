package MyAPI.General;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;

import jsat.SimpleDataSet;
import jsat.classifiers.CategoricalData;
import jsat.classifiers.DataPoint;
import jsat.linear.DenseVector;
import jsat.linear.MatrixStatistics;
import jsat.linear.Vec;
import MyAPI.General.myComparator.ValueComparator_Integer_DES;
import MyCustomedHaoop.ValueClass.DID_Score;

public class General_JSat {
	
	public static ArrayList<float[]> rerank_meanShift(ArrayList<Integer> usedRanks, float[] lats, float[] lons, double bandScaleFactor, int maxIteration) throws IOException{
		//*** meanShift clustering on usedRanks' geoLocation *******//
		ArrayList<DataPoint> dataPoints = new ArrayList<DataPoint>(usedRanks.size());
		for (int i=0;i<usedRanks.size();i++){
			int doc=usedRanks.get(i);
			double[] data=General.latlonToCartesian(lats[doc],lons[doc]); //photoName from 1!!
			dataPoints.add(new DataPoint(new DenseVector(data), new int[0], new CategoricalData[0]));
		}
		SimpleDataSet dataSet = new SimpleDataSet(dataPoints);
		//clustering
		MeanShift_my MSClustering= new MeanShift_my();
		MSClustering.setScaleBandwidthFactor(bandScaleFactor);
		MSClustering.setMaxIterations(maxIteration);
		try{
			List<List<DataPoint>> clusters= MSClustering.cluster(dataSet);
			HashMap<float[],Integer> centriod_clSize=new HashMap<float[],Integer>();
//			ArrayList<ArrayList<float[]>> cluster_latlons=new ArrayList<ArrayList<float[]>>();
			for(List<DataPoint> cluster:clusters){
				Vec mean = MatrixStatistics.meanVector(new SimpleDataSet(cluster));
				float[] centriod=General.CartesianTolatlon(mean.arrayCopy());
				centriod_clSize.put(centriod, cluster.size());
				//for test
//				ArrayList<float[]> cluster_latlon=new ArrayList<float[]>() ;
//				for(DataPoint dataPoint:cluster){
//					float[] latlon=General.CartesianTolatlon(dataPoint.getNumericalValues().arrayCopy());
//					cluster_latlon.add(latlon);
//				}
//				cluster_latlons.add(cluster_latlon);
			}
			//***** sort centriod_clSize *********
			ValueComparator_Integer_DES mvCompartor = new ValueComparator_Integer_DES(centriod_clSize);
			TreeMap<float[],Integer> centriod_clSize_Des = new TreeMap<float[],Integer>(mvCompartor);
			centriod_clSize_Des.putAll(centriod_clSize);
			return new ArrayList<float[]>(centriod_clSize_Des.keySet());
		}catch(ArithmeticException e){
			if(e.getMessage().equalsIgnoreCase("Invalid bandwith given, bandwith must be a positive number, not 0.0")){// all data in dataSet is the same, so bandwith will be 0!!
				System.err.println("fixed err in rerank_meanShift: "+e.getMessage());
				System.err.println("all data in dataSet is the same, so bandwith will be 0!!");
				ArrayList<float[]> only1 =new ArrayList<float[]>();
				int doc=usedRanks.get(0);
				float[] latlon={lats[doc],lons[doc]};
				only1.add(latlon);
				return only1;
			}else{
				System.err.println(e.getMessage());
				throw e;
			}
		}
	}
	
	public static void rerank_meanShift_retrunRankScore(ArrayList<Integer> docIDs, ArrayList<Float> docScores, 
			ArrayList<ArrayList<DID_Score>> topLocationDocsList, ArrayList<float[]> topLocs, int num_topLocations, float[] lats, float[] lons, 
			double bandScaleFactor, int maxIteration, 
			boolean is1U1P, long[] userIDs_0, int[] userIDs_1) throws IOException, InterruptedException{
		//return reranked result: rank and score(photo's cluster's Size)
		//*** meanShift clustering on usedRanks' geoLocation *******//
		ArrayList<DataPoint> dataPoints = new ArrayList<DataPoint>(docIDs.size());
		for (int i=0;i<docIDs.size();i++){
			int doc=docIDs.get(i);
			double[] data=General.latlonToCartesian(lats[doc],lons[doc]); //photoName from 1!!
			dataPoints.add(new DataPoint(new DenseVector(data), new int[0], new CategoricalData[0], i));//use i as docInd in DataPoint
		}
		SimpleDataSet dataSet = new SimpleDataSet(dataPoints);
		//clustering
		MeanShift_my MSClustering= new MeanShift_my();
		MSClustering.setScaleBandwidthFactor(bandScaleFactor);
		MSClustering.setMaxIterations(maxIteration);
		try{
			ArrayList<List<DataPoint>> clusters= new ArrayList<List<DataPoint>>(MSClustering.cluster(dataSet));
			ArrayList<Float> clusterScores=new ArrayList<Float>();
			for(List<DataPoint> thisCluster:clusters){
				//remove same user photo in one cluster
				if (is1U1P) {
					make1U1P_forTopDocsScores_DP(thisCluster, docIDs, userIDs_0, userIDs_1);
				}
				//make ranking-score for this cluster
				float thisClusterScore=0;
				for (DataPoint dataPoint : thisCluster) {
					thisClusterScore+=docScores.get(dataPoint.dataPointID);
				}
				clusterScores.add(thisClusterScore);//use cluster-member's summed score as score
			}
			//***** sort cluster_clSize *********
			ArrayList<List<DataPoint>> clusters_top= new ArrayList<List<DataPoint>>(clusters.size()); ArrayList<Float> clusterScores_top=new ArrayList<Float>(clusters.size());
			General_IR.rank_get_AllSortedDocScores_ArraySort(clusters, clusterScores, clusters_top, clusterScores_top,"DES");
			//***** get  reRankedRes, estimated locations *********
			for (int clusterind=0; clusterind<Math.min(num_topLocations,clusters_top.size());clusterind++) {
				List<DataPoint> oneCluster = clusters_top.get(clusterind);
				//get centriod
				if (oneCluster.size()==1) {//this cluster only have 1 doc
					int docIndex_in_docIDs=oneCluster.get(0).dataPointID;
					topLocs.add(new float[]{lats[docIDs.get(docIndex_in_docIDs)],lons[docIDs.get(docIndex_in_docIDs)]});
				}else{
					Vec mean = MatrixStatistics.meanVector(new SimpleDataSet(oneCluster));
					float[] centriod=General.CartesianTolatlon(mean.arrayCopy());
					topLocs.add(centriod);
				}
				//add top ranked docs
				ArrayList<DID_Score> oneLocDocs=new ArrayList<DID_Score>();
				for (DataPoint dataPoint : oneCluster) {
					int docIndex_in_docIDs=dataPoint.dataPointID;
					oneLocDocs.add(new DID_Score(docIDs.get(docIndex_in_docIDs), docScores.get(docIndex_in_docIDs)));
				}
				topLocationDocsList.add(oneLocDocs);
			}
		}catch(ArithmeticException e){
			if(e.getMessage().equalsIgnoreCase("Invalid bandwith given, bandwith must be a positive number, not 0.0")){// all data in dataSet is the same, so bandwith will be 0!!
				System.err.println("fixed err in rerank_meanShift: "+e.getMessage());
				System.err.println("all data in dataSet is the same, so bandwith will be 0!!");
				//make only1Location, all doc are from same location
				topLocs.add(new float[]{lats[docIDs.get(0)],lons[docIDs.get(0)]});
				//add top ranked docs
				ArrayList<DID_Score> oneLocDocs=new ArrayList<DID_Score>();
				for (int i=0;i<docIDs.size();i++) {
					oneLocDocs.add(new DID_Score(docIDs.get(i), docScores.get(i)));
				}
				topLocationDocsList.add(oneLocDocs);
			}else{
				System.err.println(e.getMessage());
				throw e;
			}
		}
	}
	
	public static void make1U1P_forTopDocsScores_DP(List<DataPoint> dataPoints, ArrayList<Integer> docIDs, long[] userIDs_0, int[] userIDs_1) {	
		//1 user only contribute 1 photo in the rank list
		HashSet<Long> userIDs_0_exist=new HashSet<Long>(); HashSet<Integer> userIDs_1_exist=new HashSet<Integer>();
		for(int i=0;i<dataPoints.size();i++){
			int doc=docIDs.get(dataPoints.get(i).dataPointID);
			if(userIDs_0_exist.add(userIDs_0[doc])==false && userIDs_1_exist.add(userIDs_1[doc])==false){//this user is already exist, remove!
				dataPoints.remove(i);
				i--; //after remove, the array list is immediately changed!, so next one should still be i  
			}
		}
	}
}
