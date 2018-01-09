package Lire;
//package utils.extract;
//
//
//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.FileWriter;
//import java.io.PrintStream;
//import java.util.ArrayList;
//import java.util.Comparator;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.TreeMap;
//
//import net.semanticmetadata.lire.DocumentBuilder;
//
//import org.apache.lucene.analysis.standard.StandardAnalyzer;
//import org.apache.lucene.document.Document;
//import org.apache.lucene.index.IndexReader;
//import org.apache.lucene.queryParser.MultiFieldQueryParser;
//import org.apache.lucene.queryParser.QueryParser;
//import org.apache.lucene.search.IndexSearcher;
//import org.apache.lucene.search.Query;
//import org.apache.lucene.search.ScoreDoc;
//import org.apache.lucene.search.TopScoreDocCollector;
//import org.apache.lucene.store.FSDirectory;
//import org.apache.lucene.util.Version;
//
//import utils.LanguageModel;
//import utils.Tweet;
//
//import com.csvreader.CsvWriter;
//
//public class Reader {
//	
//
//	private static IndexReader reader = null;
//	
//	StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
//	
//	private static String query = "red";
//	
//	public static String dir = "O:\\Work\\PhD\\Datasets\\MIRFLICKR25K\\";
//
//	public static void main(String[] args) {
//		try {
//			
//			Query q = new MultiFieldQueryParser(Version.LUCENE_CURRENT, 
//			                                        new String[] {"tags", "description"},
//			                                        new StandardAnalyzer(Version.LUCENE_CURRENT)).parse(query);
//			
//			int hitsPerPage = 100;
//			reader = IndexReader.open(FSDirectory.open(new File(dir + "index/")));
//		    IndexSearcher searcher = new IndexSearcher(reader);
//		    TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
//		    searcher.search(q, collector);
//		    ScoreDoc[] hits = collector.topDocs().scoreDocs;
//		    
//		    Map<String, String> lmStrings = new HashMap<String, String>();
//		    		    
//		    System.out.println("Found " + hits.length + " hits.");
//		    String r = "";
//		    for(int i=0;i<hits.length;++i) {
//		      int docId = hits[i].doc;
//		      Document d = searcher.doc(docId);
//		      
//		      System.out.println(d.get("descriptorImageIdentifier") + " " + d.get(DocumentBuilder.FIELD_NAME_SURF_LOCAL_FEATURE_HISTOGRAM));
//		      
//		      //r += d.get("tweet") + " " + d.get("title-description") + " " + d.get("tags") + " " + d.get("comments") + " " +d.get("hashtags");
//		      
//		     
//		      
//		    }
//		    
//		    lmStrings.put("1", r );
//
//		    searcher.close();
//		    
//System.out.println("data read");
//
///*
//			
//			LanguageModel.buildLM(lmStrings, true, false);
//			Map<String, Map<String, Double>> weights = LanguageModel.klDivergence();
//			
//			System.out.println("lm built");
//			
//			for (String s : weights.keySet()) {
//				Map<String, Double> probs = weights.get(s);
//				
//				CsvWriter writer = new CsvWriter(
//						new FileWriter(dir+"data/lm-lucene/"+s+"_lm-score.csv"), ',');
//				writer.setForceQualifier(true);
//				
//				for (String w : probs.keySet()) {
//					List<String> valuesCsv = new ArrayList<String>();
//		        	valuesCsv.add(w);
//		        	valuesCsv.add(String.valueOf(probs.get(w)));
//					writer.writeRecord(valuesCsv.toArray(new String[] {}));
//				}
//				
//				writer.close();
//			}
//			
//			System.out.println("files: " + dir+"data/lm-lucene/");
//			
//			PrintStream out = new PrintStream(new FileOutputStream(dir+"data/lm-lucene/_output.txt"), true);
//			
//			for (String id : lmStrings.keySet()) {
//				String v = lmStrings.get(id);
//				
//				Map<String, Double> probs = weights.get(String.valueOf(id));
//				String stringRep = lmStrings.get(String.valueOf(id));
//				
//		        ValueComparator bvc =  new ValueComparator(probs);
//		        TreeMap<String,Double> sorted_map = new TreeMap<String,Double>(bvc);
//		        sorted_map.putAll(probs);
//		        
//		        out.println(stringRep);
//		        int cnt=0;
//		        for (String s : sorted_map.keySet()) {
//		        	out.println("\t"+s + " (" + probs.get(s) + ")");
//		        	cnt++;
//		        	if (cnt==5) {
//		        		break;
//		        	}
//		        }
//		        out.println("*************************************************************");
//		        out.println();
//			}
//			
//			out.close();
//			*/
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//}
//
//
//class ValueComparator implements Comparator<String> {
//
//    Map<String, Double> base;
//    public ValueComparator(Map<String, Double> base) {
//        this.base = base;
//    }
//
//    // Note: this comparator imposes orderings that are inconsistent with equals.    
//    public int compare(String a, String b) {
//        if (base.get(a) >= base.get(b)) {
//            return -1;
//        } else {
//            return 1;
//        } // returning 0 would merge keys
//    }
//}
