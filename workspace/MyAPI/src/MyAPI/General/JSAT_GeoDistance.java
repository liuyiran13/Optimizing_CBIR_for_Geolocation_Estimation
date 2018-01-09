package MyAPI.General;

import java.io.File;
import java.util.ArrayList;
import com.almworks.sqlite4java.SQLParts;
import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import jsat.SimpleDataSet;
import jsat.classifiers.CategoricalData;
import jsat.classifiers.DataPoint;
import jsat.clustering.MeanShift;
import jsat.linear.DenseVector;
import jsat.linear.Vec;
import jsat.linear.distancemetrics.DistanceMetric;

public class JSAT_GeoDistance implements DistanceMetric{

	@Override
    public double dist(Vec a, Vec b)
    {
		String Method="GreatCircle";
        double geoDistance=-1;
		//Check for illegal input
        double Lat1=a.get(0); 
        double Lon1=a.get(1);
        double Lat2=b.get(0);
        double Lon2=b.get(1);
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

    @Override
    public boolean isSymmetric()
    {
        return true;
    }

    @Override
    public boolean isSubadditive()
    {
        return true;
    }

    @Override
    public boolean isIndiscemible()
    {
        return true;
    }

    @Override
    public double metricBound()
    {
        return Double.POSITIVE_INFINITY;
    }

    @Override
    public String toString()
    {
        return "Geo Distance";
    }

    @Override
    public JSAT_GeoDistance clone()
    {
        return new JSAT_GeoDistance();
    }
    
	public static void main(String[] args) throws SQLiteException { //to test
		JSAT_GeoDistance geoD=new JSAT_GeoDistance();
		double[] a1={21.02,30.2}; double[] b2={21.02,30.21};
		DenseVector a=new DenseVector(a1);
		DenseVector b=new DenseVector(b2);
		System.out.println(geoD.dist(a, b));
		String metaDataPath="D:\\xinchaoli\\Desktop\\My research\\Database\\FlickrPhotos\\image-meta\\FlickrPhotoMeta.db";
		//set dataBase
        SQLParts sql = new SQLParts("SELECT * FROM photoMeta WHERE photoIndex=?"); //without "WHERE", this will look for all the records!
		SQLiteConnection db_read_Q = new SQLiteConnection(new File(metaDataPath));
		db_read_Q.open(); 
		SQLiteStatement stmt_Q = db_read_Q.prepare(sql); 
		//read photos GPS into memory
		int totalPhotoNum=3185258;
		System.out.println("total photos in the dataBase:"+totalPhotoNum);
		int[] random=General.randIndex(totalPhotoNum);
		int SelNum=200;
		float[] lats=new float[SelNum]; 
		float[] lons=new float[SelNum]; 
		for(int i=0;i<SelNum;i++){
			stmt_Q.bind(1, random[i]);
			while (stmt_Q.step()) {
				String[] lat_lonQuery= stmt_Q.columnString(2).split(","); 
				lats[i]=Float.valueOf(lat_lonQuery[0]);
				lons[i]=Float.valueOf(lat_lonQuery[1]);
			}
			stmt_Q.reset();
		}
		stmt_Q.dispose();db_read_Q.dispose();
		System.out.println("done!");
		//*** meanShift clustering on usedRanks' geoLocation *******//
		ArrayList<DataPoint> dataPoints = new ArrayList<DataPoint>(lats.length);
		for (int i=0;i<lats.length;i++){
			double[] latlon={lats[i],lons[i]};
			dataPoints.add(new DataPoint(new DenseVector(latlon), new int[0], new CategoricalData[0]) );
		}
		SimpleDataSet dataSet = new SimpleDataSet(dataPoints);
		//clustering
		JSAT_GeoDistance disMatrix_geo=new JSAT_GeoDistance();
		MeanShift MSClustering= new MeanShift(disMatrix_geo);
		MSClustering.setScaleBandwidthFactor(0.1);
		int[] clusterLabel=new int[dataSet.getSampleSize()];
		MSClustering.cluster(dataSet, clusterLabel);
		System.out.println(clusterLabel.length);
		

	}

}
