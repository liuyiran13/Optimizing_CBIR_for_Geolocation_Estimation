package MyAPI.Obj;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * add more function to DataInput, DataOutput, e.g., write int
 * @author xinchaoli
 *
 */
public class DataInOutput_Functions{
	
	public static void writeString(String str, DataOutput out) throws IOException{
		byte[] data=str.getBytes("UTF-8");
		out.writeInt(data.length);
		out.write(data);
	}
	
	public static String readString(DataInput in) throws IOException{
		int length=in.readInt();
		byte[] data=new byte[length];
		in.readFully(data);
		String str=new String(data,"UTF-8");
		return str;
	}
	
	public static <T extends Writable> void writeList(List<T> list, DataOutput out) throws IOException{
		out.writeInt(list.size());
		writeList_onlyContent(list, out);
	}
	
	public static <T extends Writable> LinkedList<T> readList(DataInput in, Class<T> clazz) throws IOException{
		int arrLength=in.readInt();
		LinkedList<T> ObjArr=readKnownSizeList(in, clazz, arrLength);
		return ObjArr;
	}
	
	private static <T extends Writable> void writeList_onlyContent(List<T> list, DataOutput out) throws IOException{
		for (T one : list) {
			one.write(out);
		}
	}
	private static <T extends Writable> LinkedList<T> readKnownSizeList(DataInput in, Class<T> clazz, int size) throws IOException{
		LinkedList<T> res=new LinkedList<>();
		for(int i=0;i<size;i++){
			res.add(readObject(in, clazz));
		}
		return res;
	}
	
	public static <T extends Writable> void writeArr(T[] arr, DataOutput out) throws IOException{
		out.writeInt(arr.length);
		writeArr_onlyContent(arr, out);
	}
	
	public static <T extends Writable> T[] readArr(DataInput in, Class<T> clazz) throws IOException{
		int arrLength=in.readInt();
		T[] ObjArr=readKnownSizeArr(in, clazz, arrLength);
		return ObjArr;
	}
	
	public static <T extends Writable> void writeShortSizeArr(T[] arr, DataOutput out) throws IOException{
		if (arr!=null) {
			out.writeShort(arr.length);
			writeArr_onlyContent(arr, out);
		}else{//use arrLength==-1 to mark null
			out.writeShort(-1);
		}
		
	}
	
	public static <T extends Writable> T[] readShortSizeArr(DataInput in, Class<T> clazz) throws IOException{
		int arrLength=in.readShort();
		if (arrLength>=0) {
			T[] ObjArr=readKnownSizeArr(in, clazz, arrLength);
			return ObjArr;
		}else {//use arrLength==-1 to mark null
			return null;
		}
		
	}
	
	private static <T extends Writable> void writeArr_onlyContent(T[] arr, DataOutput out) throws IOException{
		for (T one : arr) {
			one.write(out);
		}
	}
	
	@SuppressWarnings("unchecked")
	private static <T extends Writable> T[] readKnownSizeArr(DataInput in, Class<T> clazz, int size) throws IOException{
		T[] ObjArr=(T[]) Array.newInstance(clazz, size);
		for(int i=0;i<size;i++){
			ObjArr[i]=readObject(in, clazz);
		}
		return ObjArr;
	}
	
	public static <T extends Writable> T readObject(DataInput in, Class<T> clazz) throws IOException{
		try {
			T res=clazz.newInstance();
			res.readFields(in);
			return res;
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
			throw new IOException("InstantiationException or IllegalAccessException in WritableArr");
		}
	}
	
	public static <T extends Writable> void writeArrArr(T[][] arr, DataOutput out) throws IOException{
		out.writeInt(arr.length);
		for (T[] one : arr) {
			writeArr(one, out);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T extends Writable> T[][] readArrArr(DataInput in, Class<T> clazz) throws IOException{
		int ArrLength=in.readInt();
		T[][] ObjArr=(T[][]) Array.newInstance(clazz, ArrLength);
		for(int i=0;i<ArrLength;i++){
			ObjArr[i]=readArr(in, clazz);
		}
		return ObjArr;
	}
	
	public static <T extends Writable> void writeShortSizeArrArr(T[][] arr, DataOutput out) throws IOException{
		out.writeShort(arr.length);
		for (T[] one : arr) {
			writeShortSizeArr(one, out);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T extends Writable> T[][] readShortSizeArrArr(DataInput in, Class<T> clazz) throws IOException{
		int ArrLength=in.readShort();
		T[][] ObjArr=(T[][]) Array.newInstance(clazz, ArrLength);
		for(int i=0;i<ArrLength;i++){
			ObjArr[i]=readShortSizeArr(in, clazz);
		}
		return ObjArr;
	}
	
	public static void writeByteArr(byte[] arr, DataOutput out) throws IOException{
		out.writeInt(arr.length);
		out.write(arr);
	}
	
	public static byte[] readByteArr(DataInput in) throws IOException{
		byte[] arr=new byte[in.readInt()];
		in.readFully(arr);
		return arr;
	}
	
	public static void writeIntArr(int[] arr, DataOutput out) throws IOException{
		out.writeInt(arr.length);
		for (int i : arr) {
			out.writeInt(i);
		}
	}
	
	public static int[] readIntArr(DataInput in) throws IOException{
		int[] arr=new int[in.readInt()];
		for (int i=0; i<arr.length;i++) {
			arr[i]=in.readInt();
		}
		return arr;
	}
	
	public static void writeINTArr(Integer[] arr, DataOutput out) throws IOException{
		out.writeInt(arr.length);
		for (int i : arr) {
			out.writeInt(i);
		}
	}
	
	public static Integer[] readINTArr(DataInput in) throws IOException{
		Integer[] arr=new Integer[in.readInt()];
		for (int i=0; i<arr.length;i++) {
			arr[i]=in.readInt();
		}
		return arr;
	}

	public static void writeFloatArr(float[] arr, DataOutput out) throws IOException{
		out.writeInt(arr.length);
		for (float i : arr) {
			out.writeFloat(i);
		}
	}
	
	public static float[] readFloatArr(DataInput in) throws IOException{
		float[] arr=new float[in.readInt()];
		for (int i=0; i<arr.length;i++) {
			arr[i]=in.readFloat();
		}
		return arr;
	}
	
	public static void writeFLOATArr(Float[] arr, DataOutput out) throws IOException{
		out.writeInt(arr.length);
		for (float i : arr) {
			out.writeFloat(i);
		}
	}
	
	public static Float[] readFLOATArr(DataInput in) throws IOException{
		Float[] arr=new Float[in.readInt()];
		for (int i=0; i<arr.length;i++) {
			arr[i]=in.readFloat();
		}
		return arr;
	}

	public static void writeShortArr(short[] arr, DataOutput out) throws IOException{
		out.writeInt(arr.length);
		for (int i : arr) {
			out.writeShort(i);
		}
	}
	
	public static short[] readShortArr(DataInput in) throws IOException{
		short[] arr=new short[in.readInt()];
		for (int i=0; i<arr.length;i++) {
			arr[i]=in.readShort();
		}
		return arr;
	}

}
