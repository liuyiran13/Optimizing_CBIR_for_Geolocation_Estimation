package MyAPI.General;

import ij.ImagePlus;
import ij.process.FloatProcessor;

public class General_ImageJ {

	public static void dispMatrixAsImg(float[][] data, String title, boolean isBigBlack){//data(no need pre-normalise), float[height][width], in grayScale,
		FloatProcessor fp = new FloatProcessor(data); 
		if (isBigBlack) {
			fp.invertLut();
		}
        new ImagePlus(title,fp).show(); //in fp, float[width][height], so rotate 
	}

}
