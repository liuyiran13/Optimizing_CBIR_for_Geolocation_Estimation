package MyAPI.General;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class myComparator{
	/**
	 * to USe:
	 * 		ValueComparator_DES mvCompartor = new MapValueComparator.ValueComparator_DES(docScores);
			TreeMap<Integer,Double> sorted_docScores = new TreeMap<Integer,Double>(mvCompartor);
			sorted_docScores.putAll(docScores);
	 * Tips:
	 * 		map's key can be any value, but map's value must indentical
	 *
	 */
	public static class ValueComparator_Dou_ASC implements Comparator<Object> {

		Map<?, ?> base;
		public ValueComparator_Dou_ASC(Map<?, ?> base) {
			this.base = base;
		}

		public int compare(Object a, Object b) {
			// 这里的Double必须跟HashMap<String,Double> map 中的键值类型一致
		
			if(((Double)base.get(a)).doubleValue() > ((Double)base.get(b)).doubleValue()) { //"<"是按照降序，“>”是按照升序
				return 1;
			} else if(((Double)base.get(a)).doubleValue() == ((Double)base.get(b)).doubleValue()) {// Double is object, donnot use (Double)base.get(a)) == (Double)base.get(b)
				return String.valueOf(a).compareTo(String.valueOf(b)); //prevent same value, tree map cannot insert same value with mult-keys
			} else {
				return -1;
			}
		}
	}

	public static class ValueComparator_Dou_DES implements Comparator<Object> {

		Map<?, ?> base;
		public ValueComparator_Dou_DES(Map<?, ?> base) {
			this.base = base;
		}

		public int compare(Object a, Object b) {
			// 这里的Double必须跟HashMap<String,Double> map 中的键值类型一致
		
			if(((Double)base.get(a)).doubleValue() < ((Double)base.get(b)).doubleValue()) { //"<"是按照降序，“>”是按照升序
				return 1;
			} else if(((Double)base.get(a)).doubleValue() == ((Double)base.get(b)).doubleValue()) { // Double is object, donnot use (Double)base.get(a)) == (Double)base.get(b)
				return String.valueOf(a).compareTo(String.valueOf(b)); //prevent same value, tree map cannot insert same value with mult-keys
			} else {
				return -1;
			}
		}
	}
	
	public static class ValueComparator_Float_ASC implements Comparator<Object> {

		Map<?, ?> base;
		public ValueComparator_Float_ASC(Map<?, ?> base) {
			this.base = base;
		}

		public int compare(Object a, Object b) {
			// 这里的Double必须跟HashMap<String,Double> map 中的键值类型一致
		
			if(((Float)base.get(a)).floatValue() > ((Float)base.get(b)).floatValue()) { //"<"是按照降序，“>”是按照升序
				return 1;
			} else if(((Float)base.get(a)).floatValue() == ((Float)base.get(b)).floatValue()) { // Float is object, donnot use (Double)base.get(a)) == (Double)base.get(b)
				return String.valueOf(a).compareTo(String.valueOf(b)); //prevent same value, tree map cannot insert same value with mult-keys
			} else {
				return -1;
			}
		}
	}
	
	public static class ValueComparator_HashMapSize_ASC <K,V,T extends Object> implements Comparator<K> {

		Map<K, Map<V, T>> base; //the value type in base is hashMap! compare based on hashMapSize
		public ValueComparator_HashMapSize_ASC (Map<K, Map<V, T>> base) {
			this.base = base;
		}
		
		@Override
		public int compare(K a, K b) {
			// 这里的Double必须跟HashMap<String,Double> map 中的键值类型一致
		
			if(base.get(a).size() > base.get(b).size()) { //"<"是按照降序，“>”是按照升序
				return 1;
			} else if(base.get(a).size() == base.get(b).size()) { // Float is object, donnot use (Double)base.get(a)) == (Double)base.get(b)
				return String.valueOf(a).compareTo(String.valueOf(b)); //prevent same value, tree map cannot insert same value with mult-keys
			} else {
				return -1;
			}
		}
	}
	
	public static class ValueComparator_SetSize_ASC <K,V extends Object> implements Comparator<Object> {

		Map<K, Set<V>> base; //the value type in base is Set! compare based on Set Size
		public ValueComparator_SetSize_ASC(Map<K, Set<V>> base) {
			this.base = base;
		}

		public int compare(Object a, Object b) {
			// 这里的Double必须跟HashMap<String,Double> map 中的键值类型一致
		
			if(base.get(a).size() > base.get(b).size()) { //"<"是按照降序，“>”是按照升序
				return 1;
			} else if(base.get(a).size() == base.get(b).size()) { // Float is object, donnot use (Double)base.get(a)) == (Double)base.get(b)
				return String.valueOf(a).compareTo(String.valueOf(b)); //prevent same value, tree map cannot insert same value with mult-keys
			} else {
				return -1;
			}
		}
	}

	public static class ValueComparator_Float_DES implements Comparator<Object> {

		Map<?, ?> base;
		public ValueComparator_Float_DES(Map<?, ?> base) {
			this.base = base;
		}

		public int compare(Object a, Object b) {
			// 这里的Double必须跟HashMap<String,Double> map 中的键值类型一致
		
			if(((Float)base.get(a)).floatValue() < ((Float)base.get(b)).floatValue()) { //"<"是按照降序，“>”是按照升序
				return 1;
			} else if(((Float)base.get(a)).floatValue() == ((Float)base.get(b)).floatValue()) { // Float is object, donnot use (Double)base.get(a)) == (Double)base.get(b)
				return String.valueOf(a).compareTo(String.valueOf(b)); //prevent same value, tree map cannot insert same value with mult-keys
			} else {
				return -1;
			}
		}
	}
	
	public static class ValueComparator_MasterSlave_Float_DES implements Comparator<Object> {
		//fist use value[0] as master value to compare, if they are same, then use value[1] to compare, if same, use key as string to compare
		Map<?, float[]> base;
		public ValueComparator_MasterSlave_Float_DES(Map<?, float[]> base) {
			this.base = base;
		}
		public int compare(Object a, Object b) {		
			if(base.get(a)[0] < base.get(b)[0]) { //"<"是按照降序，“>”是按照升序
				return 1;
			} else if(base.get(a)[0] == base.get(b)[0]) { // master value is the same, then compare slave value
				if(base.get(a)[1] < base.get(b)[1])
					return 1;
				else if(base.get(a)[1] == base.get(b)[1])
					return String.valueOf(a).compareTo(String.valueOf(b)); //prevent same value, tree map cannot insert same value with mult-keys
				else
					return -1;
			} else {
				return -1;
			}
		}
	}

	public static class ValueComparator_Integer_DES implements Comparator<Object> {

		Map<?, ?> base;
		public ValueComparator_Integer_DES(Map<?, ?> base) {
			this.base = base;
		}

		public int compare(Object a, Object b) {
			// 这里的Double必须跟HashMap<String,Double> map 中的键值类型一致
		
			if(((Integer)base.get(a)).intValue() < ((Integer)base.get(b)).intValue()) { //"<"是按照降序，“>”是按照升序
				return 1;
			} else if(((Integer)base.get(a)).intValue() == ((Integer)base.get(b)).intValue()) { // Float is object, donnot use (Double)base.get(a)) == (Double)base.get(b)
				return String.valueOf(a).compareTo(String.valueOf(b)); //prevent same value, tree map cannot insert same value with mult-keys
			} else {
				return -1;
			}
		}
	}
	
	public static class Comparator_IntArr implements Comparator<Integer> {

		int[] base;
		boolean DES;
		
		public Comparator_IntArr(int[] base, String model) throws InterruptedException {
			this.base = base;
			if (model.equalsIgnoreCase("DES")) {
				this.DES=true;
			}else if (model.equalsIgnoreCase("ASC")) {
				this.DES=false;
			}else {
				throw new InterruptedException("model should be DES or ASC! here model:"+model);
			}
			
		}
		
		public Comparator_IntArr(ArrayList<Integer> base, String model) throws InterruptedException {
			this.base = new int[base.size()];
			for (int i = 0; i < base.size(); i++) {
				this.base[i]=base.get(i);
			}
			if (model.equalsIgnoreCase("DES")) {
				this.DES=true;
			}else if (model.equalsIgnoreCase("ASC")) {
				this.DES=false;
			}else {
				throw new InterruptedException("model should be DES or ASC! here model:"+model);
			}
			
		}

		@Override
		public int compare(Integer a, Integer b) {
			// 这里的Double必须跟HashMap<String,Double> map 中的键值类型一致
		
			if (DES) {
				if(base[a] - base[b] < 0 ) { //"<"是按照降序，“>”是按照升序
					return 1;
				} else if(base[a] == base[b]) { 
					return String.valueOf(a).compareTo(String.valueOf(b)); //prevent same value
				} else {
					return -1;
				}
			}else {
				if(base[a] - base[b] > 0) { //"<"是按照降序，“>”是按照升序
					return 1;
				} else if(base[a] == base[b]) { // Float is object, donnot use (Double)base.get(a)) == (Double)base.get(b)
					return String.valueOf(a).compareTo(String.valueOf(b)); //prevent same value, tree map cannot insert same value with mult-keys
				} else {
					return -1;
				}
			}
			
		}
	}
	
	public static class Comparator_FloatArr implements Comparator<Integer> {

		float[] base;
		boolean DES;
		
		public Comparator_FloatArr(float[] base, String model) throws InterruptedException {
			this.base = base;
			if (model.equalsIgnoreCase("DES")) {
				this.DES=true;
			}else if (model.equalsIgnoreCase("ASC")) {
				this.DES=false;
			}else {
				throw new InterruptedException("model should be DES or ASC! here model:"+model);
			}
			
		}
		
		public Comparator_FloatArr(ArrayList<Float> base, String model) throws InterruptedException {
			this.base = new float[base.size()];
			for (int i = 0; i < base.size(); i++) {
				this.base[i]=base.get(i);
			}
			if (model.equalsIgnoreCase("DES")) {
				this.DES=true;
			}else if (model.equalsIgnoreCase("ASC")) {
				this.DES=false;
			}else {
				throw new InterruptedException("model should be DES or ASC! here model:"+model);
			}
			
		}

		@Override
		public int compare(Integer a, Integer b) {
			// 这里的Double必须跟HashMap<String,Double> map 中的键值类型一致
		
			if (DES) {
				if(base[a] - base[b] < 0 ) { //"<"是按照降序，“>”是按照升序
					return 1;
				} else if(base[a] == base[b]) { 
					return String.valueOf(a).compareTo(String.valueOf(b)); //prevent same value
				} else {
					return -1;
				}
			}else {
				if(base[a] - base[b] > 0) { //"<"是按照降序，“>”是按照升序
					return 1;
				} else if(base[a] == base[b]) { // Float is object, donnot use (Double)base.get(a)) == (Double)base.get(b)
					return String.valueOf(a).compareTo(String.valueOf(b)); //prevent same value, tree map cannot insert same value with mult-keys
				} else {
					return -1;
				}
			}
			
		}
	}

	public static class WindowsExplorerStringComparator implements Comparator<String>{
		
		/***
		 * to use: Arrays.sort(files, new WindowsExplorerStringComparator());
		 **/
		
      private String str1, str2;
      private int pos1, pos2, len1, len2;

      public int compare(String s1, String s2)
      {
        str1 = s1;
        str2 = s2;
        len1 = str1.length();
        len2 = str2.length();
        pos1 = pos2 = 0;

        int result = 0;
        while (result == 0 && pos1 < len1 && pos2 < len2)
        {
          char ch1 = str1.charAt(pos1);
          char ch2 = str2.charAt(pos2);

          if (Character.isDigit(ch1))
          {
            result = Character.isDigit(ch2) ? compareNumbers() : -1;
          }
          else if (Character.isLetter(ch1))
          {
            result = Character.isLetter(ch2) ? compareOther(true) : 1;
          }
          else
          {
            result = Character.isDigit(ch2) ? 1
                   : Character.isLetter(ch2) ? -1
                   : compareOther(false);
          }

          pos1++;
          pos2++;
        }

        return result == 0 ? len1 - len2 : result;
      }

      private int compareNumbers()
      {
        int end1 = pos1 + 1;
        while (end1 < len1 && Character.isDigit(str1.charAt(end1)))
        {
          end1++;
        }
        int fullLen1 = end1 - pos1;
        while (pos1 < end1 && str1.charAt(pos1) == '0')
        {
          pos1++;
        }

        int end2 = pos2 + 1;
        while (end2 < len2 && Character.isDigit(str2.charAt(end2)))
        {
          end2++;
        }
        int fullLen2 = end2 - pos2;
        while (pos2 < end2 && str2.charAt(pos2) == '0')
        {
          pos2++;
        }

        int delta = (end1 - pos1) - (end2 - pos2);
        if (delta != 0)
        {
          return delta;
        }

        while (pos1 < end1 && pos2 < end2)
        {
          delta = str1.charAt(pos1++) - str2.charAt(pos2++);
          if (delta != 0)
          {
            return delta;
          }
        }

        pos1--;
        pos2--; 

        return fullLen2 - fullLen1;
      }

      private int compareOther(boolean isLetters)
      {
        char ch1 = str1.charAt(pos1);
        char ch2 = str2.charAt(pos2);

        if (ch1 == ch2)
        {
          return 0;
        }

        if (isLetters)
        {
          ch1 = Character.toUpperCase(ch1);
          ch2 = Character.toUpperCase(ch2);
          if (ch1 != ch2)
          {
            ch1 = Character.toLowerCase(ch1);
            ch2 = Character.toLowerCase(ch2);
          }
        }

        return ch1 - ch2;
      }
    }

	public static void main(String[] args) throws IOException {//for debug!
		/** 
		 * ValueComparator_MasterSlave_Float_DES
		 */
		HashMap<Integer[], float[]> toSort=new HashMap<Integer[], float[]>();
		for (int i = 0; i < 10; i++) {
			float[] oneScores=new float[2];
			oneScores[0]=i+10;
			oneScores[1]=0-i;
			if (i>=0 && i<=3) {
				oneScores[0]=999;
			}
			Integer[] key=new Integer[2]; key[0]=i;
			toSort.put(key, oneScores);
		}
		for (Iterator<Integer[]> i = toSort.keySet().iterator(); i.hasNext();) {
			System.out.print(i.next()[0]+", ");	
		}
		System.out.println();
		//***** sort toSort in DES  *********
		ValueComparator_MasterSlave_Float_DES mvCompartor = new ValueComparator_MasterSlave_Float_DES(toSort);
		TreeMap<Integer[],float[]> reR_doc_GVSize_score_Des = new TreeMap<Integer[],float[]>(mvCompartor);
		reR_doc_GVSize_score_Des.putAll(toSort);
		for (Iterator<Integer[]> i = reR_doc_GVSize_score_Des.keySet().iterator(); i.hasNext();) {
			System.out.print(i.next()[0]+", ");	
		}
	}
}

