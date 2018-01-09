package MyAPI.Geo;

import java.util.List;

import MyAPI.General.General;
import MyAPI.General.General_geoRank;
import MyAPI.Geo.groupDocs.LatLon;

public class GVM_Evaluator_Loc extends GVM_Evaluator{

	private float[] isSameLocScales;
	private float[][] latlons;

	public GVM_Evaluator_Loc(String evalFlag_isSameLocScales, String latlonsPath) throws NumberFormatException, InterruptedException {
		//evalFlag: noDivideQ@1,3,5,10_1,10,100
		String[] infos=evalFlag_isSameLocScales.split("_");
		this.isSameLocScales=General.StrArrToFloatArr(infos[1].split(","));
		initialise(infos[0]);
		latlons=(float[][]) General.readObject(latlonsPath);
	}
	
	public int[] evalOneQuery_loc(int queryName, List<LatLon> topLocs){
		int[] res=new int[num_evalRadius];
		for (int i = 0; i < num_evalRadius; i++) {
			//get True-Location rank
			res[i]=General_geoRank.get_trueLocRankG(topLocs, queryName, isSameLocScales[i], latlons, num_topGroups)+1;
		}
		return res;
	}
	
	public int[] evalOneQuery_doc(int queryName, List<Integer> topDocs){
		int[] res=new int[num_evalRadius];
		for (int i = 0; i < num_evalRadius; i++) {
			//get True-Location rank
			res[i]=General_geoRank.get_trueLocRank(queryName, topDocs, num_topGroups, isSameLocScales[i], latlons)+1;
		}
		return res;
	}
	
	@Override
	void set_num_evalRadius() {
		num_evalRadius= isSameLocScales.length;
	}

	@Override
	String getEvalRadiusTitle(int evalRadInd) {
		return "isSameLocScale: "+isSameLocScales[evalRadInd];
	}

	@Override
	String getQueryTrueGroupInfo(int queryID) {
		return latlons[0][queryID]+"_"+latlons[1][queryID];
	}

}
