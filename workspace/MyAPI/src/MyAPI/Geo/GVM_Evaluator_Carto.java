package MyAPI.Geo;

import java.util.HashSet;
import java.util.List;

import MyAPI.General.General;
import MyAPI.General.General_geoRank;
import MyAPI.Geo.groupDocs.CartoDocs;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;

public class GVM_Evaluator_Carto extends GVM_Evaluator{

	HashSet<Integer>[] cartoIDs_Q;

	@SuppressWarnings("unchecked")
	public GVM_Evaluator_Carto(String evalFlag, String cartoIDs_Q_path) throws NumberFormatException, InterruptedException {
		//evalFlag: noDivideQ@1,3,5,10
		initialise(evalFlag);
		cartoIDs_Q=(HashSet<Integer>[]) General.readObject(cartoIDs_Q_path);
	}
	
	public int[] evalOneQuery_carto(int queryName, List<CartoDocs<DID_Score_ImageRegionMatch_ShortArr>> topGroups){
		return new int[]{General_geoRank.get_trueCartoRankG(topGroups, queryName, cartoIDs_Q, num_topGroups)+1};
	}
	
	@Override
	void set_num_evalRadius() {
		num_evalRadius= 1;
	}

	@Override
	String getEvalRadiusTitle(int evalRadInd) {
		return "only 1 num_evalRadius: use cartoID";
	}

	@Override
	String getQueryTrueGroupInfo(int queryID) {
		return "cartoIDs: "+cartoIDs_Q[queryID];
	}

}
