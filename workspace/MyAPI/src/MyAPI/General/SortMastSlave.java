package MyAPI.General;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;


public class SortMastSlave {
	private int[] master_Int_sort;
	private double[] master_Dou_sort;
	private float[] master_Float_sort;
	private String[] master_Str_sort;
	private String[] slave_Str_sort;
	private int[] slave_Int_sort;
	
	public int[] master_Int_sort() { 
		return master_Int_sort;
	}
	
	public double[] master_Dou_sort() { 
		return master_Dou_sort;
	}
	
	public float[] master_Float_sort() { 
		return master_Float_sort;
	}
	
	public String[] master_Str_sort() { 
		return master_Str_sort;
	}
	
	public int[] slave_Int_sort() { 
		return slave_Int_sort;
	}
	
	public String[] slave_Str_sort() { 
		return slave_Str_sort;
	}
	
	public SortMastSlave(final int[] master, int[] slave, String mode) throws NumberFormatException, IOException {

		/// sort master, slave according to ascend/descend order of master			  	
		// Create index array.
		Integer[] sortOrder = new Integer[master.length];
		for(int i=0; i<sortOrder.length; i++){
		    sortOrder[i] = i;
		}			
		
		if(mode.equalsIgnoreCase("ASC")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { 
			public int compare(Integer a, Integer b){
				return master[a]-master[b]; // aescending order
			}});
		}else if(mode.equalsIgnoreCase("DES")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { 
			public int compare(Integer a, Integer b){
				return master[b]-master[a]; // descending order
			}});
		}else{
			System.err.println("error for SortMastSlave, mode should be ASC or DES!!");
		}
		
							
		master_Int_sort=new int[sortOrder.length];
		slave_Int_sort=new int[sortOrder.length];
		for(int i=0; i<sortOrder.length; i++){
			master_Int_sort[i]=master[sortOrder[i]];
			slave_Int_sort[i]=slave[sortOrder[i]];
		}
		// sort end
	}
	
	public SortMastSlave(final int[] master, String[] slave, String mode) throws NumberFormatException, IOException {

		/// sort master, slave according to ascend/descend order of master			  	
		// Create index array.
		Integer[] sortOrder = new Integer[master.length];
		for(int i=0; i<sortOrder.length; i++){
		    sortOrder[i] = i;
		}			
		
		if(mode.equalsIgnoreCase("ASC")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { 
			public int compare(Integer a, Integer b){
				return master[a]-master[b]; // aescending order
			}});
		}else if(mode.equalsIgnoreCase("DES")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { 
			public int compare(Integer a, Integer b){
				return master[b]-master[a]; // descending order
			}});
		}else{
			System.err.println("error for SortMastSlave, mode should be ASC or DES!!");
		}
		
							
		master_Int_sort=new int[sortOrder.length];
		slave_Str_sort=new String[sortOrder.length];
		for(int i=0; i<sortOrder.length; i++){
			master_Int_sort[i]=master[sortOrder[i]];
			slave_Str_sort[i]=slave[sortOrder[i]];
		}
		// sort end
	}
	
	public SortMastSlave(final double[] master, String[] slave, String mode) throws NumberFormatException, IOException {

		/// sort master, slave according to ascend/descend order of master			  	
		// Create index array.
		Integer[] sortOrder = new Integer[master.length];
		for(int i=0; i<sortOrder.length; i++){
		    sortOrder[i] = i;
		}			
		
		if(mode.equalsIgnoreCase("ASC")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { //Compares its two arguments for order. Returns a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
			public int compare(Integer a, Integer b){
				if((master[a]-master[b])>0)// aescending order
					return 1;
				else if((master[a]-master[b])==0)
					return 0;
				else
					return -1;
			}});
		}else if(mode.equalsIgnoreCase("DES")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { 
			public int compare(Integer a, Integer b){
				if((master[b]-master[a])>0)// descending order
					return 1;
				else if((master[b]-master[a])==0)
					return 0;
				else
					return -1;
			}});
		}else{
			System.err.println("error for SortMastSlave, mode should be ASC or DES!!");
		}
		
							
		master_Dou_sort=new double[sortOrder.length];
		slave_Str_sort=new String[sortOrder.length];
		for(int i=0; i<sortOrder.length; i++){
			master_Dou_sort[i]=master[sortOrder[i]];
			slave_Str_sort[i]=slave[sortOrder[i]];
		}
		// sort end
	}
	
	public SortMastSlave(final float[] master, int[] slave, String mode) throws NumberFormatException, IOException {

		/// sort master, slave according to ascend/descend order of master			  	
		// Create index array.
		Integer[] sortOrder = new Integer[master.length];
		for(int i=0; i<sortOrder.length; i++){
		    sortOrder[i] = i;
		}			
		
		if(mode.equalsIgnoreCase("ASC")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { //Compares its two arguments for order. Returns a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
			public int compare(Integer a, Integer b){
				if((master[a]-master[b])>0)// aescending order
					return 1;
				else if((master[a]-master[b])==0)
					return 0;
				else
					return -1;
			}});
		}else if(mode.equalsIgnoreCase("DES")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { 
			public int compare(Integer a, Integer b){
				if((master[b]-master[a])>0)// descending order
					return 1;
				else if((master[b]-master[a])==0)
					return 0;
				else
					return -1;
			}});
		}else{
			System.err.println("error for SortMastSlave, mode should be ASC or DES!!");
		}
		
							
		master_Float_sort=new float[sortOrder.length];
		slave_Int_sort=new int[sortOrder.length];
		for(int i=0; i<sortOrder.length; i++){
			master_Float_sort[i]=master[sortOrder[i]];
			slave_Int_sort[i]=slave[sortOrder[i]];
		}
		// sort end
	}
	
	public SortMastSlave(final String[] master, int[] slave, String mode) throws NumberFormatException, IOException {

		/// sort master, slave according to ascend/descend order of master			  	
		// Create index array.
		Integer[] sortOrder = new Integer[master.length];
		for(int i=0; i<sortOrder.length; i++){
		    sortOrder[i] = i;
		}		
		
		int MaxLength_temp=0;
		for(int i=0;i<master.length;i++){
			if(MaxLength_temp<master[i].length()){
				MaxLength_temp=master[i].length();
			}
		}
		final int MaxLength=MaxLength_temp;
		
		if(mode.equalsIgnoreCase("ASC")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { 
			public int compare(Integer a, Integer b){
				String Uni_a=StrleftPad(master[a], 0, MaxLength, "0");
				String Uni_b=StrleftPad(master[b], 0, MaxLength, "0");
				return Uni_a.compareToIgnoreCase(Uni_b); // Ascending order
			}});
		}else if(mode.equalsIgnoreCase("DES")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { 
			public int compare(Integer a, Integer b){
				String Uni_a=StrleftPad(master[a], 0, MaxLength, "0");
				String Uni_b=StrleftPad(master[b], 0, MaxLength, "0");
				return Uni_b.compareToIgnoreCase(Uni_a); // descending order
			}});
		}else{
			System.err.println("error for SortMastSlave, mode should be ASC or DES!!");
		}
		
							
		master_Str_sort=new String[sortOrder.length];
		slave_Int_sort=new int[sortOrder.length];
		for(int i=0; i<sortOrder.length; i++){
			master_Str_sort[i]=master[sortOrder[i]];
			slave_Int_sort[i]=slave[sortOrder[i]];
		}
		// sort end
	}
	
	public SortMastSlave(final String[] master, String[] slave, String mode) throws NumberFormatException, IOException {

		/// sort master, slave according to ascend/descend order of master			  	
		// Create index array.
		Integer[] sortOrder = new Integer[master.length];
		for(int i=0; i<sortOrder.length; i++){
		    sortOrder[i] = i;
		}			
		
		int MaxLength_temp=0;
		for(int i=0;i<master.length;i++){
			if(MaxLength_temp<master[i].length()){
				MaxLength_temp=master[i].length();
			}
		}
		final int MaxLength=MaxLength_temp;
		
		if(mode.equalsIgnoreCase("ASC")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { 
			public int compare(Integer a, Integer b){
				String Uni_a=StrleftPad(master[a], 0, MaxLength, "0");
				String Uni_b=StrleftPad(master[b], 0, MaxLength, "0");
				return Uni_a.compareToIgnoreCase(Uni_b); // Ascending order
			}});
		}else if(mode.equalsIgnoreCase("DES")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { 
			public int compare(Integer a, Integer b){
				String Uni_a=StrleftPad(master[a], 0, MaxLength, "0");
				String Uni_b=StrleftPad(master[b], 0, MaxLength, "0");
				return Uni_b.compareToIgnoreCase(Uni_a); // descending order
			}});
		}else{
			System.err.println("error for SortMastSlave, mode should be ASC or DES!!");
		}
		
							
		master_Str_sort=new String[sortOrder.length];
		slave_Str_sort=new String[sortOrder.length];
		for(int i=0; i<sortOrder.length; i++){
			master_Str_sort[i]=master[sortOrder[i]];
			slave_Str_sort[i]=slave[sortOrder[i]];
		}
		// sort end
	}
   	 	
	public SortMastSlave(Integer[] master_Integer, String[] slave, String mode) throws NumberFormatException, IOException {

		final int[] master=new int[master_Integer.length];
		for(int i=0;i<master.length;i++){
			master[i]=master_Integer[i];
		}
		
		/// sort master, slave according to ascend/descend order of master			  	
		// Create index array.
		Integer[] sortOrder = new Integer[master.length];
		for(int i=0; i<sortOrder.length; i++){
		    sortOrder[i] = i;
		}			
		
		if(mode.equalsIgnoreCase("ASC")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { 
			public int compare(Integer a, Integer b){
				return master[a]-master[b]; // aescending order
			}});
		}else if(mode.equalsIgnoreCase("DES")){
			Arrays.sort(sortOrder,new Comparator<Integer>() { 
			public int compare(Integer a, Integer b){
				return master[b]-master[a]; // descending order
			}});
		}else{
			System.err.println("error for SortMastSlave, mode should be ASC or DES!!");
		}
		
							
		master_Int_sort=new int[sortOrder.length];
		slave_Str_sort=new String[sortOrder.length];
		for(int i=0; i<sortOrder.length; i++){
			master_Int_sort[i]=master[sortOrder[i]];
			slave_Str_sort[i]=slave[sortOrder[i]];
		}
		// sort end
	}

	private static String StrleftPad(String str, int padMode, int LENGTH, String addStr) { //将 str补足为长度LENGTH， padMode 0的时候左补SPACE，0以外时候右补SPACE
		String result = str;
		int i = 0;
		while (i < (LENGTH - str.length())) {
			if (padMode == 0) {
				result = addStr + result;
			} else {
				result = result + addStr;
			}
			i++;
		}

		return result;
	}
}
