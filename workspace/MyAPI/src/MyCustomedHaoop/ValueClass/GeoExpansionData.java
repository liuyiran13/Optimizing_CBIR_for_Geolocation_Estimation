package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import MyAPI.General.General;
import MyAPI.Geo.groupDocs.CartoDocs;
import MyAPI.Geo.groupDocs.LocDocs;
import MyAPI.Interface.DID;
import MyAPI.Interface.Score;
import MyAPI.Obj.DataInOutput_Functions;
import MyAPI.imagR.GTruth;

public class GeoExpansionData{
	
	public static class GTSize_ap_topDocs implements Writable{
		int groudTSize;
		float ap;
		int[] topDocs;
	
		public GTSize_ap_topDocs(int groudTSize, float ap, int[] topDocs) {
			super();
			this.groudTSize=groudTSize;
			this.ap=ap;
			this.topDocs=topDocs;
		}
		
		public GTSize_ap_topDocs() {
			// do nothing
		}
	
		public int get_groudTSize() {
			return groudTSize;
		}
		
		public float get_ap() {
			return ap;
		}
		
		public int[] get_topDocs() {
			return topDocs;
		}
	
		/* (non-Javadoc)
		 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			
			out.writeInt(groudTSize);
			out.writeFloat(ap);
			out.writeInt(topDocs.length);
			
			for(int i=0;i<topDocs.length;i++){
				out.writeInt(topDocs[i]);
			}
		}
	
		/* (non-Javadoc)
		 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
		 */
		@Override
		public void readFields(DataInput in) throws IOException {
			
			groudTSize=in.readInt();
			ap=in.readFloat();
			int topDocs_L=in.readInt();
			topDocs=new int[topDocs_L];
			
			for(int i=0;i<topDocs_L;i++){
				topDocs[i]=in.readInt();
			}
	
		}
	}
	
	public static class GTSize_Docs_GVSizes_docScores implements Writable{
		int groudTSize;
		int[] Docs;
		int[] GVSizes;
		float[] docScores;
	
		public GTSize_Docs_GVSizes_docScores(int groudTSize, int[] Docs, int[] GVSizes, float[] docScores) {
			super();
			assert Docs.length==docScores.length;
			assert GVSizes.length==docScores.length;
			this.groudTSize=groudTSize;
			this.Docs=Docs;
			this.GVSizes=GVSizes;
			this.docScores=docScores;
		}
		
		public GTSize_Docs_GVSizes_docScores() {
			// do nothing
		}
	
		public int get_groudTSize() {
			return groudTSize;
		}
	
		public int[] get_Docs() {
			return Docs;
		}
		
		public int[] get_GVSizes() {
			return GVSizes;
		}
		
		public float[] get_docScores() {
			return docScores;
		}
	
		/* (non-Javadoc)
		 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			
			out.writeInt(groudTSize);
			out.writeInt(Docs.length);
			
			for(int i=0;i<Docs.length;i++){
				out.writeInt(Docs[i]);
				out.writeInt(GVSizes[i]);
				out.writeFloat(docScores[i]);
			}
		}
	
		/* (non-Javadoc)
		 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
		 */
		@Override
		public void readFields(DataInput in) throws IOException {
			
			groudTSize=in.readInt();
			int Docs_L=in.readInt();
			Docs=new int[Docs_L];
			GVSizes=new int[Docs_L];
			docScores=new float[Docs_L];
					
			for(int i=0;i<Docs_L;i++){
				Docs[i]=in.readInt();
				GVSizes[i]=in.readInt();
				docScores[i]=in.readFloat();
			}
	
		}
	}
	
	public static class fistMatch_GTruth_Docs_GVSizes_docScores implements Writable{
		public GTruth fistMatch; //two elements, 1st: firstMatch's Rank, 2nd: firstMatch's photoName
		public ArrayList<int[]> gTruth; //ArrayList<rank_photoName>
		public int[] Docs;
		public int[] GVSizes;
		public float[] docScores;
	
		public fistMatch_GTruth_Docs_GVSizes_docScores(GTruth fistMatch, ArrayList<int[]> gTruth, int[] Docs, int[] GVSizes, float[] docScores) {
			super();
			assert Docs.length==docScores.length;
			assert GVSizes.length==docScores.length;
			this.fistMatch=fistMatch;
			this.gTruth=gTruth;
			this.Docs=Docs;
			this.GVSizes=GVSizes;
			this.docScores=docScores;
		}
		
		public fistMatch_GTruth_Docs_GVSizes_docScores() {
			// do nothing
		}
	
		@Override
		public void write(DataOutput out) throws IOException {
			fistMatch.write(out);
			int GTruth_size=gTruth.size(); int GTruth_cellLength=gTruth.get(0).length;
			out.writeInt(GTruth_size);
			out.writeInt(GTruth_cellLength);
			for (int i = 0; i < GTruth_size; i++) {
				General.Assert(gTruth.get(i).length==GTruth_cellLength, "err in fistMatch_GTruth_Docs_GVSizes_docScores! cell in GTruth should be equal length!");
				for (int k: gTruth.get(i)) {
					out.writeInt(k);
				}
			}
			
			out.writeInt(Docs.length);
			
			for(int i=0;i<Docs.length;i++){
				out.writeInt(Docs[i]);
				out.writeInt(GVSizes[i]);
				out.writeFloat(docScores[i]);
			}
		}
	
		@Override
		public void readFields(DataInput in) throws IOException {
			fistMatch=new GTruth();
			fistMatch.readFields(in);
			
			int GTruth_size=in.readInt(); 
			int GTruth_cellLength=in.readInt();
			gTruth=new ArrayList<int[]>(GTruth_size);
			for (int i = 0; i < GTruth_size; i++) {
				int[] cell=new int[GTruth_cellLength];
				for (int j = 0; j < GTruth_cellLength; j++) {
					cell[j]=in.readInt();
				}
				gTruth.add(cell);
			}
			
			int Docs_L=in.readInt();
			Docs=new int[Docs_L];
			GVSizes=new int[Docs_L];
			docScores=new float[Docs_L];
					
			for(int i=0;i<Docs_L;i++){
				Docs[i]=in.readInt();
				GVSizes[i]=in.readInt();
				docScores[i]=in.readFloat();
			}
	
		}
	}
	
	
	public static abstract class fistMatch_GTruth_Docs implements Writable{
		public GTruth fistMatch; //two elements, 1st: firstMatch's Rank, 2nd: firstMatch's photoName
		public GTruth[] gTruths; //<rank_photoName>
		public int geoDensity;
		public DID_Score_ArrArr Docs;
		
		public fistMatch_GTruth_Docs(GTruth fistMatch, List<GTruth> gTruths, int geoDensity, DID_Score_ArrArr Docs) {
			super();
			set(fistMatch, gTruths, geoDensity, Docs);
		}
		
		public fistMatch_GTruth_Docs(GTruth fistMatch, List<GTruth> gTruths, int geoDensity, ArrayList<ArrayList<DID_Score>> Docs) {
			super();
			set(fistMatch, gTruths, geoDensity, new DID_Score_ArrArr(Docs));
		}
		
		public fistMatch_GTruth_Docs() {
			// do nothing
		}
		
		protected void set(GTruth fistMatch, List<GTruth> gTruths, int geoDensity, DID_Score_ArrArr Docs){
			this.fistMatch=fistMatch;
			this.gTruths=gTruths.toArray(new GTruth[0]);
			this.geoDensity=geoDensity;
			this.Docs=Docs;
		}
		
		public int getGTSize(){
			return GTruth.getGTSize(Arrays.asList(gTruths));
		}

		@Override
		public void write(DataOutput out) throws IOException {
			fistMatch.write(out);
			DataInOutput_Functions.writeArr(gTruths, out);
			out.writeInt(geoDensity);
			Docs.write(out);
		}
	
		@Override
		public void readFields(DataInput in) throws IOException {
			fistMatch=DataInOutput_Functions.readObject(in, GTruth.class);
			gTruths=DataInOutput_Functions.readArr(in, GTruth.class);
			geoDensity=in.readInt();
			Docs=DataInOutput_Functions.readObject(in, DID_Score_ArrArr.class);
		}
		
		@Override
		public String toString(){
			return "fistMatch(rank_docID):"+fistMatch+", groudTSize:"+getGTSize()
					+", geoDensity:"+geoDensity+", groupNum:"+Docs.getArr().length+", [0-th group's 0-th doc_score]:"+Docs.getArr()[0].getArr()[0];
			
		}
		
	}
	
	public static class fistMatch_GTruth_Docs_Locations extends fistMatch_GTruth_Docs{
		public FloatArrArr topLocations;
		
		public <V extends DID & Score> fistMatch_GTruth_Docs_Locations(GTruth fistMatch, List<GTruth> gTruths, LinkedList<LocDocs<V>> Docs, int geoDensity) {
			super(fistMatch, gTruths, geoDensity, new DID_Score_ArrArr(Docs));
			this.topLocations=new FloatArrArr(LocDocs.getLocList(Docs));
		}
		
		public fistMatch_GTruth_Docs_Locations() {
			// do nothing
		}
		
		public <V extends DID & Score> void set(GTruth fistMatch, List<GTruth> gTruths, int geoDensity, LinkedList<LocDocs<V>> Docs){
			super.set(fistMatch, gTruths, geoDensity, new DID_Score_ArrArr(Docs));
			this.topLocations=new FloatArrArr(LocDocs.getLocList(Docs));
		}

		@Override
		public void write(DataOutput out) throws IOException {
			super.write(out);
			topLocations.write(out);
		}
	
		@Override
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			topLocations=DataInOutput_Functions.readObject(in, FloatArrArr.class);
		}
		
		@Override
		public String toString(){
			return super.toString()+", 0-th group location:"+General.floatArrToString(topLocations.getArrArr()[0], "_", "0.00000");
			
		}
	}
	
	public static class fistMatch_GTruth_Docs_Cartos extends fistMatch_GTruth_Docs{
		public Integer[] topCartoIDs;
				
		public <V extends DID & Score> fistMatch_GTruth_Docs_Cartos(GTruth fistMatch, List<GTruth> gTruths, LinkedList<CartoDocs<V>> Docs, int geoDensity) {
			super(fistMatch, gTruths, geoDensity, new DID_Score_ArrArr(Docs));
			this.topCartoIDs=CartoDocs.getCartoList(Docs).toArray(new Integer[0]);
		}
		
		public fistMatch_GTruth_Docs_Cartos() {
			// do nothing
		}
		
		public <V extends DID & Score> void set(GTruth fistMatch, List<GTruth> gTruths, LinkedList<CartoDocs<V>> Docs, int geoDensity) {
			super.set(fistMatch, gTruths, geoDensity, new DID_Score_ArrArr(Docs));
			this.topCartoIDs=CartoDocs.getCartoList(Docs).toArray(new Integer[0]);
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			super.write(out);
			DataInOutput_Functions.writeINTArr(topCartoIDs, out);
		}
	
		@Override
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			topCartoIDs=DataInOutput_Functions.readINTArr(in);
		}
		
		@Override
		public String toString(){
			return super.toString()+", 0-th group cartoID:"+topCartoIDs[0];
		}
	}

}
