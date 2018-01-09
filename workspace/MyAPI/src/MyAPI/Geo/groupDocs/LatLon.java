package MyAPI.Geo.groupDocs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Writable;

import MyAPI.General.General;

public class LatLon implements Writable, I_LatLon{
	public float lat;
	public float lon;
	
	public LatLon(float lat, float lon) {
		this.lat=lat;
		this.lon=lon;
	}
	
	public LatLon(float[] latlon) {
		this.lat=latlon[0];
		this.lon=latlon[1];
	}
	
	public LatLon(float[][] latlon, int id) {
		this.lat=latlon[0][id];
		this.lon=latlon[1][id];
	}
	
	public String toString(){
		return lat+"_"+lon;
	}
	
	public float[] getFloatArr(){
		return new float[]{lat,lon};
	}
	
	public float getDistInKm(LatLon ano){
		return (float) General.calculateGeoDistance(lat, lon, ano.lat, ano.lon, "GreatCircle");
	}
	
	public String getDistStrInKm(LatLon ano, String format){//format: 0.0
		return new DecimalFormat(format).format(getDistInKm(ano))+"km";
	}
	
	public static String getLatLon(float[][] latlon, int id, String delimeter){//latlon[0] is lat, latlon[1] is lon
		return latlon[0][id]+delimeter+latlon[1][id];
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		lat=in.readFloat();
		lon=in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(lat);
		out.writeFloat(lon);
	}

	@Override
	public LatLon getLatLon() {
		return new LatLon(lat, lon);
	}

}
