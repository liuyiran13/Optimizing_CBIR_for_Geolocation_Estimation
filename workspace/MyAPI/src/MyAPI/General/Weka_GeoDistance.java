package MyAPI.General;

import java.util.Enumeration;
import java.util.Vector;

import weka.core.EuclideanDistance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.Range;
import weka.core.Utils;
import weka.core.neighboursearch.PerformanceStats;


@SuppressWarnings("serial")
public class Weka_GeoDistance extends EuclideanDistance  {

	private String Method="GreatCircle"; //default use GreatCircle method to calculate distance
	private Instances m_Data;
	
	/** The range of attributes to use for calculating the distance. */
	private Range m_AttributeIndices = new Range("first-last");
	
	public Weka_GeoDistance() {
	    
	}
	public Weka_GeoDistance(Instances data) {
	    setInstances(data);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public Enumeration listOptions() {
		return null;
	}

	@Override
	public void setOptions(String[] options) throws Exception {
	    Method = Utils.getOption('M', options);
	    if (Method.equalsIgnoreCase("Cartesian"))
	    	Method="Cartesian";
	}

	@Override
	public String[] getOptions() {
	    Vector<String>	result;
	    result = new Vector<String>();
	    result.add("-M");
	    result.add(Method);
	    
	    return result.toArray(new String[result.size()]);
	}

	@Override
	public void setInstances(Instances insts) {
		m_Data = insts;
	}

	@Override
	public Instances getInstances() {
		return m_Data;
	}

	@Override
	public void setAttributeIndices(String value) {
		
	}

	@Override
	public String getAttributeIndices() {
	    return m_AttributeIndices.getRanges();
	}  

	@Override
	public void setInvertSelection(boolean value) {
		
	}

	@Override
	public boolean getInvertSelection() {
		return false;
	}

	@Override
	public double distance(Instance first, Instance second) {
		double distance = 0;
	    //1st att should be latitude, 2nd att should be longitude
	    double Lat1=first.value(0);double Lon1=first.value(1);
	    double Lat2=second.value(0);double Lon2=second.value(1);
	    distance= calculateGeoDistance(Lat1,Lon1,Lat2,Lon2, Method);
	    return distance;
	}

	@Override
	public double distance(Instance first, Instance second,PerformanceStats stats) {

		return distance(first, second);
	}

	@Override
	public double distance(Instance first, Instance second, double cutOffValue) {
		return distance(first, second);
	}

	@Override
	public double distance(Instance first, Instance second, double cutOffValue,PerformanceStats stats) {
		return distance(first, second);
	}

	

	@Override
	public void update(Instance ins) {
		
	}

	@Override
	public void postProcessDistances(double[] distances) {
		
	}
	
	private double calculateGeoDistance(double Lat1,double Lon1,double Lat2,double Lon2, String method){
		double geoDistance=-1;
		//Check for illegal input
		boolean outofbounds=Math.abs(Lat1)>90|Math.abs(Lon1)>180|Math.abs(Lat2)>90|Math.abs(Lon2)>180;
		if (outofbounds){
			System.err.println("calculateGeoDistance error!..lat long data out of normal boundary");
			return geoDistance;
		}
		
		if (Lon1 < 0)
	    	Lon1 = Lon1 + 360;
	    if (Lon2 < 0)
	    	Lon2 = Lon2 + 360;
	    
	    if(Method.equalsIgnoreCase("Cartesian")){
	    	double km_per_deg_la = 111.3237;
	        double km_per_deg_lo = 111.1350;
	        double km_la = km_per_deg_la * (Lat1-Lat2);
	        double dif_lo;
	        // Always calculate the shorter arc.
	        if (Math.abs(Lon1-Lon2) > 180)
	            dif_lo = Math.abs(Lon1-Lon2)-180;
	        else
	            dif_lo = Math.abs(Lon1-Lon2);
	        double km_lo = km_per_deg_lo * dif_lo * Math.cos((Lat1+Lat2)*Math.PI/360);
	        geoDistance = Math.sqrt(Math.pow(km_la, 2)+ Math.pow(km_lo,2));
	    }else{
	    	double R_aver = 6374;
	    	double deg2rad = Math.PI/180;
	        Lat1 = Lat1 * deg2rad;
	        Lon1 = Lon1 * deg2rad;
	        Lat2 = Lat2 * deg2rad;
	        Lon2 = Lon2 * deg2rad;
	        geoDistance = R_aver * Math.acos(Math.cos(Lat1)*Math.cos(Lat2)*Math.cos(Lon1-Lon2) + Math.sin(Lat1)*Math.sin(Lat2));
	    }

	    return geoDistance;
	}

}
