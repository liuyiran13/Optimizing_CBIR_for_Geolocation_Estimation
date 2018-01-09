package Test;

public class general_Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		
//		ArrayList<ArrayList<Double>> pFeatElList = new ArrayList<ArrayList<Double>>();
//		for (int i=0;i<64;i++){
//			ArrayList<Double> one= new ArrayList<Double>();
//			one.add(10.0);
//			one.add(1.0);
//			pFeatElList.add(one);
//		}
//		pFeatElList.get(0).add(20.0);
//		pFeatElList.get(0).add(5.0);
//		//before sort
//		ArrayList<Double> oneDoublList=pFeatElList.get(0);
//		Double [] countries = oneDoublList.toArray(new Double[oneDoublList.size()]);
//		System.out.println("before sort:"+General.DouArrToString(countries, ","));
//		//with sort
//		Collections.sort(pFeatElList.get(0));
//		oneDoublList=pFeatElList.get(0);
//		countries = oneDoublList.toArray(new Double[oneDoublList.size()]);
//		System.out.println("with sort:"+General.DouArrToString(countries, ","));
		
		String temp="/disk1/mapred.local.dir/taskTracker/yliu/jobcache/job_201306100736_0974/attempt_201306100736_0974_r_000041_0/work/conceptData.file";
		System.out.println(temp.substring(0, temp.indexOf("conceptData.file")));
	}

}
