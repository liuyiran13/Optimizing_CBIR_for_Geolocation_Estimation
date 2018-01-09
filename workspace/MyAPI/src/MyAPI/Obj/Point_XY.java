package MyAPI.Obj;

import java.util.ArrayList;
import java.util.HashSet;

import MyCustomedHaoop.ValueClass.MatchFeat_VW;

public class Point_XY{
	public int pointInd;
	public short x;
	public short y;
	
	public Point_XY(int pointInd, short x, short y) {
		this.pointInd=pointInd;
		this.x=x;
		this.y=y;
	}
	
	public static HashSet<Integer> getUniPoints(ArrayList<Point_XY> list) {
		HashSet<Integer> uniPoints=new HashSet<Integer>();
		for (Point_XY p : list) {
			uniPoints.add(p.pointInd);
		}
		return uniPoints;
	}
	
	public static HashSet<Integer> getUniPoints(ArrayList<Point_XY> list, HashSet<Integer> mostFrequentVW_inOneLine, ArrayList<MatchFeat_VW> goodMatches) {
		//function used in General_BoofCV.weightMatchByHistAndAngle
		HashSet<Integer> uniPoints=new HashSet<Integer>();
		for (Point_XY p : list) {
			if (mostFrequentVW_inOneLine.contains(goodMatches.get(p.pointInd).vw)) {
				uniPoints.add(p.pointInd);
			}
		}
		return uniPoints;
	}
}
