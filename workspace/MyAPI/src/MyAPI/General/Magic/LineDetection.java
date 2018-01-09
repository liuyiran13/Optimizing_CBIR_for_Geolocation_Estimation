package MyAPI.General.Magic;

import MyAPI.General.General;
import MyAPI.Obj.HistMultiD_Sparse_equalSizeBin_forFloat;
import MyAPI.Obj.Point_XY;

public class LineDetection extends HistMultiD_Sparse_equalSizeBin_forFloat<Point_XY>{
	/*
	 * r = x * cos(theta) + y * sin(theta)  
	 * since we does not know the theta angle and r value, we have to calculate all hough space(each theta has one r) for each pixel point  
	 * then we got the max possible theta and r pair.  
	 */
	protected int pointNum;
	
	public LineDetection(float binStep_theta, float[] binSetup_r){
		//binSetup_theta: 0~pi, binSetup_r:-maxLength~maxLength
		super(true, true);
		makeEqualBins(new float[][]{new float[]{0,(float) Math.PI,binStep_theta},binSetup_r}, "0.0", null);
		pointNum=0;
	}
	
	@Override
	public void iniHist() {
		super.iniHist();
		pointNum=0;
	}
	
	@Override
	protected void addOneSample_assignOneBin(float[] notUsed, Point_XY sample) {
		for (int binInd0 = 0; binInd0 < bins[0].length-1; binInd0++) {
			//one point generate bins[0].length-1 ge theta and r pair, each vote for one bin, the last bin for thetha is not used, as it is >= PI
			float theta=bins[0][binInd0];
			float r=(float) (sample.x*Math.cos(theta)+sample.y*Math.sin(theta));
			int binInd1=General.getBinInd_equalBin(eachDimBegEndStp[1], bins[1].length, r);
			int binIndInOneDim=General.getOneDimInd_forMutiDimArr(new int[]{binInd0,binInd1}, eachDimSize);
			General.updateMap(hist, binIndInOneDim, 1);
			if (saveSample) {
				General.updateMap(samplesInBins, binIndInOneDim, sample);
			}
			sampleNum++;
		}
		pointNum++;
	}
	
	protected void addOneSample_assignTwoBins(float[] notUsed, Point_XY sample) {
		for (int binInd0 = 0; binInd0 < bins[0].length; binInd0++) {//one point generate bins[0].length ge theta and r pair, each vote for one bin
			float theta=bins[0][binInd0];
			float r=(float) (sample.x*Math.cos(theta)+sample.y*Math.sin(theta));
			int[] binInd1=General.getBinInd_equalBin_assignTwoBins(eachDimBegEndStp[1], bins[1].length, r, bins_middle[1]);
			for (int i : binInd1) {
				int binIndInOneDim=General.getOneDimInd_forMutiDimArr(new int[]{binInd0,i}, eachDimSize);
				General.updateMap(hist, binIndInOneDim, 1);
				if (saveSample) {
					General.updateMap(samplesInBins, binIndInOneDim, sample);
				}
			}
			sampleNum++;
		}
		pointNum++;
	}
}
