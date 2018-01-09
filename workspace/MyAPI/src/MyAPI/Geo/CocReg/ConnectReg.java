package MyAPI.Geo.CocReg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import MyAPI.General.Magic.ConnectComponent;
import MyAPI.Obj.DID_FeatInds_Score;
import MyCustomedHaoop.ValueClass.SURFpoint_onlyLoc;

public class ConnectReg {

	SURFpoint_onlyLoc[] find_Pos;
	int pointDist_thr_squared;
	HashMap<CocRegs, Boolean> isNeighbors; //CocRegs here only for two points
	
	public ConnectReg(SURFpoint_onlyLoc[] find_Pos, int pointDist_thr) {
		this.find_Pos=find_Pos;
		this.pointDist_thr_squared=pointDist_thr*pointDist_thr;
		isNeighbors=new HashMap<>();
	}
	
	
	public List<Set<DID_FeatInds_Score>> getConnectComponents(ArrayList<DID_FeatInds_Score> locMatches){//regID in locMatches should always in the ascending order!
		ConnectComponent<DID_FeatInds_Score> connectComp=new ConnectComponent<>();
		//add vertex
		for (DID_FeatInds_Score one:locMatches) {
			connectComp.addOneVertex(one);
		}
		for (int i = 0; i < locMatches.size(); i++) {
			for (int j = i+1; j < locMatches.size(); j++) {
				if (getIsNeighbor(locMatches.get(i).featInd_Q, locMatches.get(j).featInd_Q)) {
					connectComp.addOneEdge(locMatches.get(i), locMatches.get(j));
				}
			}
		}
		return connectComp.getConnectComps();
	}
	
	private boolean getIsNeighbor(int fInd0, int fInd1){
		CocRegs oneReg=new CocRegs(new Integer[]{fInd0, fInd1});
		Boolean isNeighbor=isNeighbors.get(oneReg);
		if (isNeighbor==null) {//not calculated before
			isNeighbor=isNeighbor(oneReg);
			isNeighbors.put(oneReg, isNeighbor);
		}
		return isNeighbor;
	}
	
	private boolean isNeighbor(CocRegs oneReg){
		return find_Pos[oneReg.regIDs[0]].isNeighbor_Euclidian(find_Pos[oneReg.regIDs[1]], pointDist_thr_squared);
	}

}
