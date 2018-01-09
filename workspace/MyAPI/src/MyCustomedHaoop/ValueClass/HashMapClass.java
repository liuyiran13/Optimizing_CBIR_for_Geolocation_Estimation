package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.Writable;

public class HashMapClass {
	
	public static class HashMap_IntFloat implements Writable{
		HashMap<Integer,Float> hashMap=new HashMap<Integer,Float>();
	
		public HashMap_IntFloat(HashMap<Integer,Float> hashMap) {
			super();
			this.hashMap = hashMap;
		}
		
		public HashMap_IntFloat() {
			// do nothing
		}
	
		public HashMap<Integer,Float> getHashMap() {
			return hashMap;
		}
		
		public void set(HashMap<Integer,Float> hashMap) {
			this.hashMap = hashMap;
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			int map_length=hashMap.size();
			out.writeInt(map_length);
			for(Integer key:hashMap.keySet()){
				out.writeInt(key);
				out.writeFloat(hashMap.get(key));
			}
		}
	
		/* (non-Javadoc)
		 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
		 */
		@Override
		public void readFields(DataInput in) throws IOException {
			int map_length = in.readInt();
			hashMap=new HashMap<Integer,Float>(map_length);
			for(int i=0; i<map_length; i++){
				int key=in.readInt();
				float value=in.readFloat();
				hashMap.put(key, value);
			}
		}
	}

	public static class HashMap_Int_ByteArrList_Short implements Writable{
		
		//use out.writeShort!! ,so each ArrayList<byte[]> need to be 0~32767
		HashMap<Integer,ArrayList<byte[]>> hashMap=new HashMap<Integer,ArrayList<byte[]>>(); 
		int byteArrLength;
		
		public HashMap_Int_ByteArrList_Short(HashMap<Integer,ArrayList<byte[]>> hashMap) {
			super();
			this.hashMap = hashMap;
			for(int key:hashMap.keySet()){
				ArrayList<byte[]> ByteList=hashMap.get(key);
				byteArrLength=ByteList.get(0).length;
				for(byte[] onebyteArr:ByteList){//each element in byteArrs should be equal length!
					assert byteArrLength == onebyteArr.length : "error in HashMap<Integer,ArrayList<byte[]>>, key:"+key
							+", byteArrLength:"+byteArrLength+", onebyteArr.length:"+onebyteArr.length;
				}
			}
		}
		
		public HashMap_Int_ByteArrList_Short() {
			// do nothing
		}
	
		public HashMap<Integer,ArrayList<byte[]>> getHashMap() {
			return hashMap;
		}
		
		public void set(HashMap<Integer,ArrayList<byte[]>> hashMap) {
			this.hashMap = hashMap;
			for(int key:hashMap.keySet()){
				ArrayList<byte[]> ByteList=hashMap.get(key);
				byteArrLength=ByteList.get(0).length;
				for(byte[] onebyteArr:ByteList){//each element in byteArrs should be equal length!
					assert byteArrLength == onebyteArr.length : "error in HashMap<Integer,ArrayList<byte[]>>, key:"+key
							+", byteArrLength:"+byteArrLength+", onebyteArr.length:"+onebyteArr.length;
				}
			}
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			int map_length=hashMap.size();
			out.writeInt(map_length);
			out.writeInt(byteArrLength);
			for(Integer key:hashMap.keySet()){
				out.writeInt(key);
				ArrayList<byte[]> ByteList=hashMap.get(key);
				int ByteList_length=ByteList.size();
				out.writeShort(ByteList_length);// ByteList is usually short, so
				for(byte[] onebyteArr:ByteList){//each element in byteArrs should be equal length!
					out.write(onebyteArr);
				}
			}
		}
	
		/* (non-Javadoc)
		 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
		 */
		@Override
		public void readFields(DataInput in) throws IOException {
			int map_length = in.readInt();
			hashMap=new HashMap<Integer,ArrayList<byte[]>>(map_length);
			byteArrLength = in.readInt();
			for(int i=0; i<map_length; i++){
				int key=in.readInt();
				int ByteList_length=in.readShort();
				ArrayList<byte[]> ByteList=new ArrayList<byte[]>(ByteList_length);
				for(int bi=0; bi<ByteList_length; bi++){
					//when byteArrs.add, it add oneByteArr's address not the value, so if not creat a new one, all element in byteArrs will be the same one as present oneByteArr
					byte[] oneByteArr=new byte[byteArrLength];
					in.readFully(oneByteArr);
					ByteList.add(oneByteArr);
				}
				hashMap.put(key, ByteList);
			}
		}
	}

	public static class HashMap_Int_ByteArrList implements Writable{
		
		//use out.writeShort!! ,so each ArrayList<byte[]> need to be 0~32767
		HashMap<Integer,ArrayList<byte[]>> hashMap=new HashMap<Integer,ArrayList<byte[]>>(); 
		int byteArrLength;
		
		public HashMap_Int_ByteArrList(HashMap<Integer,ArrayList<byte[]>> hashMap) {
			super();
			this.hashMap = hashMap;
			for(int key:hashMap.keySet()){
				ArrayList<byte[]> ByteList=hashMap.get(key);
				byteArrLength=ByteList.get(0).length;
				for(byte[] onebyteArr:ByteList){//each element in byteArrs should be equal length!
					assert byteArrLength == onebyteArr.length : "error in HashMap<Integer,ArrayList<byte[]>>, key:"+key
							+", byteArrLength:"+byteArrLength+", onebyteArr.length:"+onebyteArr.length;
				}
			}
		}
		
		public HashMap_Int_ByteArrList() {
			// do nothing
		}
	
		public HashMap<Integer,ArrayList<byte[]>> getHashMap() {
			return hashMap;
		}
		
		public void set(HashMap<Integer,ArrayList<byte[]>> hashMap) {
			this.hashMap = hashMap;
			for(int key:hashMap.keySet()){
				ArrayList<byte[]> ByteList=hashMap.get(key);
				byteArrLength=ByteList.get(0).length;
				for(byte[] onebyteArr:ByteList){//each element in byteArrs should be equal length!
					assert byteArrLength == onebyteArr.length : "error in HashMap<Integer,ArrayList<byte[]>>, key:"+key
							+", byteArrLength:"+byteArrLength+", onebyteArr.length:"+onebyteArr.length;
				}
			}
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			int map_length=hashMap.size();
			out.writeInt(map_length);
			out.writeInt(byteArrLength);
			for(Integer key:hashMap.keySet()){
				out.writeInt(key);
				ArrayList<byte[]> ByteList=hashMap.get(key);
				int ByteList_length=ByteList.size();
				out.writeInt(ByteList_length);// ByteList is usually short, so
				for(byte[] onebyteArr:ByteList){//each element in byteArrs should be equal length!
					out.write(onebyteArr);
				}
			}
		}
	
		/* (non-Javadoc)
		 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
		 */
		@Override
		public void readFields(DataInput in) throws IOException {
			int map_length = in.readInt();
			hashMap=new HashMap<Integer,ArrayList<byte[]>>(map_length);
			byteArrLength = in.readInt();
			for(int i=0; i<map_length; i++){
				int key=in.readInt();
				int ByteList_length=in.readInt();
				ArrayList<byte[]> ByteList=new ArrayList<byte[]>(ByteList_length);
				for(int bi=0; bi<ByteList_length; bi++){
					//when byteArrs.add, it add oneByteArr's address not the value, so if not creat a new one, all element in byteArrs will be the same one as present oneByteArr
					byte[] oneByteArr=new byte[byteArrLength];
					in.readFully(oneByteArr);
					ByteList.add(oneByteArr);
				}
				hashMap.put(key, ByteList);
			}
		}
	}

	
}
