package SURFVW;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import MyAPI.General.General;
import MyAPI.General.General_Lucene;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class luceneIndexForImageVW {
	
  public static void main(String[] args) throws IOException, java.io.FileNotFoundException {
	  
	  
	  DecimalFormat percformat= (DecimalFormat)NumberFormat.getPercentInstance();
	  percformat.applyPattern("00.000%");//设置百分率的输出形式，形如00.*,根据需要设定。
	  double percentage;
	    
	  String line1Photo=null; 
	
	  int MergeFactor=2; int intervel=1000;  //disp, file number
	
	  int PhotoNums=3*1000*1000; //3 milion photos
	  
//	  String base_Path="Q:\\IndexWorkSpace\\";
	  String base_Path="/tudelft.net/staff-bulk/ewi/mm/D-MIRLab/XinchaoLi/IndexWorkSpace/";
	  
	  String VWSym="VW20000";
	  String field="photoVW";
	  
	  //** Build Index **//   
	  File index_path=new File(base_Path+"SURFVW_"+VWSym+"_LucceneIndexFile"); // To store an index on disk, 
	  General.deleteAll(index_path); //delete index files
	  LogDocMergePolicy logMergePolicy= new LogDocMergePolicy(); logMergePolicy.setMergeFactor(MergeFactor);
	  Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);//Specify the analyzer for tokenizing text. The same analyzer should be used for indexing and searching
	  IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40, analyzer);
	  config.setMergePolicy(logMergePolicy);
	  config.setOpenMode(IndexWriterConfig.OpenMode.CREATE); //overwrite 
	  Directory index = FSDirectory.open(index_path);//create the index
	  IndexWriter indexWriter = new IndexWriter(index, config);

	  long startTime=System.currentTimeMillis(),endTime;   //start time  
//	  String imgVW_path="Q:\\SURFVW\\ClassifySURF_res_"+VWSym+"_All"; 
	  String imgVW_path="/tudelft.net/staff-bulk/ewi/mm/D-MIRLab/XinchaoLi/SURFVW/ClassifySURF_res_"+VWSym+"_All"; 
	  BufferedReader inStrVW = new BufferedReader(new InputStreamReader(new FileInputStream(imgVW_path), "UTF-8"));
	  int lineIndex=0;
	  while((line1Photo=inStrVW.readLine())!=null){ //line1Photo: photoName\t1 2 3 .....
		  String[] onePhoto=line1Photo.split("\t");
		  General_Lucene.addDoc(indexWriter, onePhoto[1], field);//docID from 0, indentical with photoName
		  lineIndex++;
		  if ((lineIndex)%intervel==0){
			  //disp info
			  endTime=System.currentTimeMillis(); //end time 
			  percentage=(double)lineIndex/PhotoNums;
			  System.out.println("indexing photoLine-"+lineIndex+", "+percformat.format(percentage)+", running time:"+(endTime-startTime)/1000/60+"mins");
		  }
	  }inStrVW.close();
	  indexWriter.close();
	  endTime=System.currentTimeMillis(); //end time 
	  System.out.println("indexing finished ......, total running time:"+(endTime-startTime)/1000/60+"mins");
	  	  
	  //** Read Index **//   
	  AtomicReader indexread= SlowCompositeReaderWrapper.wrap(DirectoryReader.open(index));
	  PrintWriter outStr = new PrintWriter(new OutputStreamWriter(new FileOutputStream(base_Path+"SURFVW_"+VWSym+"_LuceneIndex_TermFreq",false), "UTF-8"));
	  int DocNums=indexread.numDocs();
	  System.out.println("DocNums:"+DocNums);
	  String termText; int termFre;
	  int termNum=0; //term number
//	  TermsEnum terms=indexread.terms(field).intersect(compiled, startTerm);
//	  while (terms.next()){
//		  Term term = terms.term();
//		  termText = term.text();
//		  termFre = indexread.docFreq(term);
//		  termNum++;
//		  percentage=((double)termFre)/DocNums;
//		  outStr.println(termText+", "+termFre+", "+percformat.format(percentage));
//	  }
	  System.out.println("Total "+DocNums+" indexed photos, unique terms: "+termNum);
	  outStr.println("Total "+DocNums+" indexed photos, unique terms: "+termNum);
	  indexread.close();outStr.close();
    

  }
}
