package MyAPI.Obj;

import java.util.LinkedList;

import org.apache.hadoop.mapreduce.Reducer.Context;

import MyAPI.General.General;

public class KMean_InMemory {
	private float[][] data_inFloat;
	private LinkedList<float[]> data_inFloat_list;
	private boolean disp;
	
	public KMean_InMemory(float[][] data, boolean disp){
		if (data==null) {
			data_inFloat_list=new LinkedList<float[]>();
		}else {
			this.data_inFloat=data;
		}
		this.disp=disp;
	}
	
	public void addDataSample(float[] oneSample){
		data_inFloat_list.add(oneSample);
	}
	
	@SuppressWarnings("rawtypes")
	public float[][] makeRes(int clusterNum, int maxInterNum, float[][] previousCenters, Context context_hadoopReducer) throws InterruptedException {
		if (data_inFloat==null) {
			data_inFloat=data_inFloat_list.toArray(new float[0][]);
		}
		if (previousCenters==null) {
			General.dispInfo_ifNeed(disp, "", "previousCenters is null, now making random sellected centers from the data");
			//******* rand select k=clusterNum center ******
			boolean isDuplicated=true;
			while (isDuplicated) {
				int[] randInd=General.randIndex(data_inFloat.length);
				int[] selectedInd=General.selectArrInt(randInd, null, clusterNum);
				previousCenters=General.selectArr(data_inFloat, selectedInd, 0).toArray(new float[0][]);
				isDuplicated=General.isDuplicated_Rows(previousCenters, randInd, (float) 0.001, disp);
				General.dispInfo_ifNeed(disp, "", "isDuplicated in random sellected centers:"+isDuplicated);
			}
		}
		//******* do k-mean clustering *******
		General.dispInfo_ifNeed(disp, "", "start k-mean clustering ......");
		long startTime=System.currentTimeMillis();
		int dispInter=(int) Math.ceil((double)maxInterNum/10);
		for (int loop_i = 0; loop_i < maxInterNum; loop_i++) {
			float[][] newCenters=General.ini_floatArrArr(clusterNum, data_inFloat[0].length);
			int[] clusterSize=new int[clusterNum];
			for (int i = 0; i < data_inFloat.length; i++) {
				int centerInd=General.assignFeatToCenter(data_inFloat[i], previousCenters);
				General.addFloatArr(newCenters[centerInd],data_inFloat[i]);
				clusterSize[centerInd]++;
			}
			for (int i = 0; i < clusterNum; i++) {
				General.elementDiv(newCenters[i], clusterSize[i]);
			}
			previousCenters=newCenters;
			if (disp && (loop_i%dispInter==0)) {
				System.out.println("k-mean clustering, loop:"+loop_i+" finished!  ....  "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
				System.out.println("\t current cluster Size: min_"+General.getMin_ind_val(clusterSize)[1]+", max_"+General.getMax_ind_val(clusterSize)[1]+", ave_"+(float)General.sum_IntArr(clusterSize)/clusterNum);
				if (context_hadoopReducer!=null) {//prevent reducer being kill due to long Time InMemory Kmean
					context_hadoopReducer.progress();
				}
			}
		}
		General.dispInfo_ifNeed(disp, "", "k-mean clustering done!  ....  "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		return previousCenters;
	}
}
