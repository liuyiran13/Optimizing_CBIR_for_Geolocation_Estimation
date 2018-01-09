package MyCustomedHaoop.ValueClass;

import org.apache.hadoop.io.IntWritable;

import MyCustomedHaoop.ValueClass.ArrWritableClassCollection.HESig_ShortArr_Arr;


public class ComposeWritableClassCollection{
	
	public static class PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr extends AbstractTwoWritable<PhotoPointsLoc,DID_Score_ImageRegionMatch_ShortArr_Arr> {
		public PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr() {
	        super(PhotoPointsLoc.class,DID_Score_ImageRegionMatch_ShortArr_Arr.class);
	    }
	    
	    public PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr(PhotoPointsLoc photoInfo, DID_Score_ImageRegionMatch_ShortArr_Arr rank) {
	        super(photoInfo, rank, PhotoPointsLoc.class, DID_Score_ImageRegionMatch_ShortArr_Arr.class);
	    }
	}
	
	public static class QID_IntList_FloatList extends AbstractTwoWritable<IntWritable,IntList_FloatList> {
		public QID_IntList_FloatList() {
	        super(IntWritable.class,IntList_FloatList.class);
	    }
	    
	    public QID_IntList_FloatList(int queryID, IntList_FloatList rank) {
	        super(new IntWritable(queryID), rank, IntWritable.class, IntList_FloatList.class);
	    }
	}
	
	public static class QID_PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr extends AbstractTwoWritable<IntWritable,PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr> {
		public QID_PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr() {
	        super(IntWritable.class,PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr.class);
	    }
	    
	    public QID_PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr(int queryID, PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr queryInfo_rank) {
	        super(new IntWritable(queryID), queryInfo_rank, IntWritable.class, PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr.class);
	    }
	}
	
	public static class IntArr_SURFfeat_ShortArr_Arr extends AbstractTwoWritable<IntArr,SURFfeat_ShortArr_Arr> {
		public IntArr_SURFfeat_ShortArr_Arr() {
	        super(IntArr.class,SURFfeat_ShortArr_Arr.class);
	    }
	    
		public IntArr_SURFfeat_ShortArr_Arr(IntArr ints, SURFfeat_ShortArr_Arr feats) {
	        super(ints, feats, IntArr.class,SURFfeat_ShortArr_Arr.class);
	    }
		
	    public IntArr_SURFfeat_ShortArr_Arr(int[] ints, SURFfeat_ShortArr_Arr feats) {
	        super(new IntArr(ints), feats, IntArr.class,SURFfeat_ShortArr_Arr.class);
	    }
	    
	    public IntArr_SURFfeat_ShortArr_Arr(int[] integers, SURFfeat[][] ObjArrArr, byte[][] aggSigs) {
	        super(new IntArr(integers), new SURFfeat_ShortArr_Arr(ObjArrArr, aggSigs), IntArr.class,SURFfeat_ShortArr_Arr.class);
	    }
	}
	
	public static class IntArr_HESig_ShortArr_Arr extends AbstractTwoWritable<IntArr,HESig_ShortArr_Arr> {
		public IntArr_HESig_ShortArr_Arr() {
	        super(IntArr.class,HESig_ShortArr_Arr.class);
	    }
	    
		public IntArr_HESig_ShortArr_Arr(IntArr ints, HESig_ShortArr_Arr feats) {
	        super(ints, feats, IntArr.class,HESig_ShortArr_Arr.class);
	    }
		
	    public IntArr_HESig_ShortArr_Arr(int[] ints, HESig_ShortArr_Arr feats) {
	        super(new IntArr(ints), feats, IntArr.class,HESig_ShortArr_Arr.class);
	    }
	    
	}

}

