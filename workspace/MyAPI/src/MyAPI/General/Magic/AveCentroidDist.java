package MyAPI.General.Magic;

import java.awt.Point;
import java.util.LinkedList;

public class AveCentroidDist {
	
	LinkedList<Point> points;
	
	public AveCentroidDist() {
		points=new LinkedList<>();
	}
	
	public void addOnePoint(int[] xy){
		addOnePoint(xy[0], xy[1]);
	}
	
	public void addOnePoint(int x, int y){
		points.add(new Point(x, y));
	}

	public double getAveCentroidDist(){
		Point centroid=new Point();
		for (Point one : points) {
			centroid.translate(one.x, one.y);
		}
		double[] centroid_ca=new double[]{centroid.x/points.size(), centroid.y/points.size()};
		double dist=0;
		for (Point one : points) {
			dist+=Point.distance(one.x, one.y, centroid_ca[0], centroid_ca[1]);
		}
		return dist/points.size();
	}
}
