package MyAPI.General;

import java.io.IOException;
import java.util.HashMap;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

public class General_Lucene {

	public static void addDoc(IndexWriter w, String value, String field) throws IOException {
		/**
		 * addDoc(IndexWriter w, String value, String field)
		 */
		Document doc = new Document();
		doc.add(new TextField(field, value, Field.Store.NO));
		w.addDocument(doc);
	}
	
	public static HashMap<String,Integer> termFreq(String[] doc) throws IOException {
		/**
		 * Calculate term freq in one doc
		 */
		HashMap<String,Integer> termFreq=new HashMap<String,Integer>();
		for(String term:doc){
			if (termFreq.containsKey(term)){
				termFreq.put(term, termFreq.get(term)+1);
			}else{
				termFreq.put(term, 1);
			}
		}
		return termFreq;
	}

	public static String[] tagsToQuery(String Tags, WhitespaceAnalyzer analyzer) throws IOException {
//		  IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_35, analyzer);
//		  Directory index = new RAMDirectory();
//		  IndexWriter w = new IndexWriter(index, config);
//		  Document doc = new Document();
//		  doc.add(new Field("tagsToQuery", Tags, Field.Store.YES, Field.Index.ANALYZED,Field.TermVector.YES));
//		  w.addDocument(doc);w.close();
//		    
//		  IndexReader reader = IndexReader.open(index);
//		  TermFreqVector termfreqV=reader.getTermFreqVector(0, "tagsToQuery");
//		  String[] terms=termfreqV.getTerms();
//		  int[] freq=termfreqV.getTermFrequencies();
		  int sum=0;
//		  for(int i=0;i<freq.length;i++){ // total tag num in one photo
//			  sum=sum+freq[i];
//		  }
//		    
		  String[] Tag=new String[sum];
//		  int tag_i=0;
//		  for(int i=0;i<terms.length;i++){
//			  if(freq[i]==1){
//				  Tag[tag_i]=terms[i];
//				  tag_i++;
//			  }else{
//				  for (int j=1;j<=freq[i];j++){
//					  Tag[tag_i]=terms[i];
//					  tag_i++;
//				  }
//			  }
//		  }
		  return Tag;
	  }

	public static void main(String[] args) throws IOException {//debug
		
		//*****		test termFreq	******//
		String[] doc={"t1","t1","t2","t1","t2","t23"};
		HashMap<String,Integer> termFreq=termFreq(doc);
		for(String key:termFreq.keySet()){
			System.out.println(key+":"+termFreq.get(key));
		}
	}
}
