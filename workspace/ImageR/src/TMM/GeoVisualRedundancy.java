package TMM;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.io.MapFile;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_geoRank;
import MyAPI.Obj.FindEqualSizedBin;

import com.almworks.sqlite4java.SQLiteException;

public class GeoVisualRedundancy {

	public static void main(String[] args) throws IOException, SQLiteException, ClassNotFoundException, InterruptedException {
		makeMecTurkData();
	}
	
	@SuppressWarnings("unchecked")
	public static void makeMecTurkData() throws IOException, SQLiteException, ClassNotFoundException, InterruptedException {
		String soursePath="O:/MediaEval13/";
		
		float[][] latlons=(float[][]) General.readObject(soursePath+"MEval13_latlons.floatArr");

		//load image mapFiles
		String imageMapFilesPath="Q:/Photos_MEva13_9M_MapFiles/";
		MapFile.Reader[] imgMapFiles= General_Hadoop.openAllMapFiles(new String[]{imageMapFilesPath});
		System.out.println("load mapFiles finished! tot-"+imgMapFiles.length);
				
		float isSameLocScale=(float) 0.01; //1km
		int randQNum=1000;
		//rand select n from query_Q0
		HashMap<Integer, Integer> s_to_s_Q0=(HashMap<Integer, Integer>) General.readObject(soursePath+"Querys_S_to_S/Q0");
		HashSet<Integer> selected= General.randSelect(new Random(), s_to_s_Q0.size(), randQNum, 0);
		int q_ind=0; HashMap<Integer, ArrayList<Integer>> Q_geoNeighbors=new HashMap<Integer, ArrayList<Integer>>();
		int dispInter=1000; int[] binsFor_GeoNeighborNum=new int[]{0,15,70,200,800,2000,5000};//  new int[]{0,15,70,200,800,2000,5000}
		int[] hist_GeoNeighborNum=new int[binsFor_GeoNeighborNum.length+1];
		long startTime=System.currentTimeMillis(); int max_GeoNeighborNum=0;
		for (Entry<Integer, Integer> one : s_to_s_Q0.entrySet()) {
			if (selected.contains(q_ind)) {
				ArrayList<Integer> geoNeighbors= General_geoRank.findGeoNeighbors(one.getKey(), isSameLocScale, latlons);
				Q_geoNeighbors.put(one.getKey(), geoNeighbors);
				//neighbor num statistics
				int binInd=General.getBinInd_linear(binsFor_GeoNeighborNum,geoNeighbors.size());
				hist_GeoNeighborNum[binInd]++;
				max_GeoNeighborNum=Math.max(max_GeoNeighborNum, geoNeighbors.size());
			}
			q_ind++;
			if (q_ind%dispInter==0) {
				System.out.println("processed querys:"+q_ind+", random select:"+Q_geoNeighbors.size()
						+", max_GeoNeighborNum:"+max_GeoNeighborNum+", GeoNeighborNum hists on "+General.IntArrToString(binsFor_GeoNeighborNum, "_")+": "+General.IntArrToString(hist_GeoNeighborNum, "_"));
				System.out.println("time: "+General.dispTime(System.currentTimeMillis()-startTime, "min")+"\n");
			}
		}
		System.out.println("done! total processed querys:"+q_ind+", random select:"+Q_geoNeighbors.size()
				+", max_GeoNeighborNum:"+max_GeoNeighborNum+", GeoNeighborNum hists on "+General.IntArrToString(binsFor_GeoNeighborNum, "_")+": "+General.IntArrToString(hist_GeoNeighborNum, "_"));
	
		//*** find bin size for equal assign  *********
		FindEqualSizedBin findEqualSizedBin=new FindEqualSizedBin(General.makeRange(new int[]{0,5000,5}), true);
		for (Entry<Integer, ArrayList<Integer>> oneQ : Q_geoNeighbors.entrySet()) {
			findEqualSizedBin.addOneSample(oneQ.getValue().size());
		}
		System.out.println(findEqualSizedBin.makeRes(6, Q_geoNeighbors.size()));
	}

}
