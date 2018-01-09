package MyCustomedHaoop.ValueClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import MyCustomedHaoop.ValueClass.HashMapClass.HashMap_Int_ByteArrList;


public class forTest {

	/**
	 * test customed hadoop writable class 
	 * @throws IOException s
	 */
	public static void main(String[] args) throws IOException {
		
		
//		//MapFile Configure
//		int MapFileindInter=1; //MapFile index interval
//		String MapFile_path="Q:\\IndexWorkSpace\\MapFileTest";
//		Configuration conf = new Configuration();
//        FileSystem hdfs  = FileSystem.get(conf);
//        
////        //test HashMap_IntFloat
////        test_HashMap_IntFloat(MapFileindInter,MapFile_path,conf,hdfs);
//        
//        //test HashMap_Int_ByteArrList
//        test_HashMap_Int_ByteArrList(MapFileindInter,MapFile_path,conf,hdfs);
        
        int[][] tt=new int[2][];int[] kk={0,1,1,2,2,3};int[] k={0,1};
        tt[0]=kk;tt[1]=k;
        System.out.println(tt.length);
        	
	}
	
	@SuppressWarnings("deprecation")
	public static void test_HashMap_IntFloat(int MapFileindInter,String MapFile_path,Configuration conf,FileSystem hdfs) throws IOException{
		
		//********* write test data **************//
		//set to-test class
        IntWritable MapFile_key = new IntWritable();
        HashMapClass.HashMap_IntFloat MapFile_value = new HashMapClass.HashMap_IntFloat();
        //set MapFile.Writer
        MapFile.Writer MapFileWriter = new MapFile.Writer(conf, hdfs, MapFile_path, MapFile_key.getClass(), MapFile_value.getClass());
        MapFileWriter.setIndexInterval(MapFileindInter);
        //write test data
        for(int fi=0;fi<10;fi++){
        	HashMap<Integer,Float> hashMap=new HashMap<Integer,Float>();
        	for(int i=0;i<5;i++){
        		hashMap.put(i, new Float(i+0.123456789));
        	}
        	MapFile_key.set(fi);
        	MapFile_value.set(hashMap);
        	MapFileWriter.append(MapFile_key, MapFile_value);
        }
        MapFileWriter.close();
        
        //********* read test data **************//
        MapFile.Reader MapFileReader=new MapFile.Reader(hdfs, MapFile_path, conf);
        for(int fi=0;fi<10;fi++){
        	MapFile_key.set(fi);
	        if(MapFileReader.get(MapFile_key, MapFile_value)!=null){
	        	System.out.println("MapFile_key.get():"+MapFile_key.get());
	        	System.out.println(MapFile_value.getHashMap().toString());
	        }
        }
        MapFileReader.close();
	}
	
	@SuppressWarnings("deprecation")
	public static void test_HashMap_Int_ByteArrList(int MapFileindInter,String MapFile_path,Configuration conf,FileSystem hdfs) throws IOException{
		
		//********* write test data **************//
		//set to-test class
        IntWritable MapFile_key = new IntWritable();
        HashMap_Int_ByteArrList MapFile_value = new HashMap_Int_ByteArrList();
        //set MapFile.Writer
        MapFile.Writer MapFileWriter = new MapFile.Writer(conf, hdfs, MapFile_path, MapFile_key.getClass(), MapFile_value.getClass());
        MapFileWriter.setIndexInterval(MapFileindInter);
        //write test data
        for(int fi=0;fi<10;fi++){
        	HashMap<Integer,ArrayList<byte[]>> hashMap=new HashMap<Integer,ArrayList<byte[]>>();
        	for(int i=0;i<5;i++){
        		ArrayList<byte[]> temp=new ArrayList<byte[]>();
        		String tempStr="12";
        		temp.add(tempStr.getBytes());
        		hashMap.put(i,temp);
        	}
        	System.out.println("MapFile_key.get():"+fi);
        	System.out.println(new String(hashMap.get(0).get(0)));
        	MapFile_key.set(fi);
        	MapFile_value.set(hashMap);
        	MapFileWriter.append(MapFile_key, MapFile_value);
        }
        MapFileWriter.close();
        
        //********* read test data **************//
        MapFile.Reader MapFileReader=new MapFile.Reader(hdfs, MapFile_path, conf);
        for(int fi=0;fi<10;fi++){
        	MapFile_key.set(fi);
	        if(MapFileReader.get(MapFile_key, MapFile_value)!=null){
	        	System.out.println("MapFile_key.get():"+MapFile_key.get());
	        	System.out.println(new String(MapFile_value.getHashMap().get(0).get(0)));
	        }
        }
        MapFileReader.close();
	}

}
