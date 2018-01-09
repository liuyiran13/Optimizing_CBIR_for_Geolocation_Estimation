package MyAPI.Geo.groupDocs;

import java.util.LinkedList;
import java.util.List;

import MyAPI.Interface.DID;

public class LocDocs <T extends DID > extends GroupDocs<T> implements I_LatLon{
	public LatLon latlon;
	
	public LocDocs(List<T> docs, LatLon latlon){
		super(docs);
		this.latlon=latlon;
	}
	
	public LocDocs(GroupDocs<T> docs,LatLon latlon){
		super(docs.docs);
		this.latlon=latlon;
	}
	
	public LocDocs(LocDocs<T> that){
		super(that.docs);
		this.latlon=that.latlon;
	}

	@Override
	public LatLon getLatLon() {
		return latlon;
	}
	
	public static <K extends I_LatLon> LinkedList<float[]> getLocList(List<K> Docs){
		LinkedList<float[]> res=new LinkedList<float[]>();
		for (K one : Docs) {
			res.add(one.getLatLon().getFloatArr());
		}
		return res;
	}
	
	public static <K extends I_LatLon> LinkedList<LatLon> getLocationList(List<K> Docs){
		LinkedList<LatLon> res=new LinkedList<LatLon>();
		for (K one : Docs) {
			res.add(one.getLatLon());
		}
		return res;
	}	
}
