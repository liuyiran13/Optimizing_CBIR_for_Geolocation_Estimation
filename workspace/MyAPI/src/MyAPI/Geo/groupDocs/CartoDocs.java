package MyAPI.Geo.groupDocs;

import java.util.LinkedList;
import java.util.List;

import MyAPI.Interface.DID;

public class CartoDocs <T extends DID> extends GroupDocs<T> implements I_CartoID{
	public int cartoID;
	
	public CartoDocs(List<T> docs, int cartoID){
		super(docs);
		this.cartoID=cartoID;
	}
	
	public CartoDocs(GroupDocs<T> docs, int cartoID){
		super(docs.docs);
		this.cartoID=cartoID;
	}
	
	public CartoDocs(CartoDocs<T> that){
		super(that.docs);
		this.cartoID=that.cartoID;
	}
	
	@Override
	public int getCartoID() {
		return cartoID;
	}
	
	public static <K extends DID> LinkedList<Integer> getCartoList(List<CartoDocs<K>> Docs){
		LinkedList<Integer> res=new LinkedList<Integer>();
		for (CartoDocs<K> one : Docs) {
			res.add(one.getCartoID());
		}
		return res;
	}
}
