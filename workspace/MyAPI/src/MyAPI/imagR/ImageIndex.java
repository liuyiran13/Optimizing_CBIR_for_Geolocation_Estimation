package MyAPI.imagR;

import java.io.IOException;

import MyCustomedHaoop.ValueClass.TVector;

public abstract class ImageIndex {
	
	//********************** data ******************
	public ImageIndexInfo indexInfo;
	
	public abstract TVector getOneTVector(int vw) throws IllegalArgumentException, IOException;
	
}
