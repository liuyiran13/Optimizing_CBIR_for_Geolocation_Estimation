package MyAPI.General;
import java.util.BitSet;
import java.math.BigInteger;

import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;
import org.ejml.ops.NormOps;


public class General_EJML {

	public static DenseMatrix64F douArrToDenseMatrix(double[] feat, boolean isRow) {
		DenseMatrix64F featMatrix=isRow?new DenseMatrix64F(1,feat.length):new DenseMatrix64F(feat.length,1);
		for (int i = 0; i < feat.length; i++) {
			featMatrix.set(i, feat[i]);
		}
		
		return featMatrix;
	}
	
	public static DenseMatrix64F floatArrToDenseMatrix(float[] feat, boolean isRow) {
		DenseMatrix64F featMatrix=isRow?new DenseMatrix64F(1,feat.length):new DenseMatrix64F(feat.length,1);
		for (int i = 0; i < feat.length; i++) {
			featMatrix.set(i, feat[i]);
		}
		return featMatrix;
	}
	
	public static DenseMatrix64F douArrArrToDenseMatrix(double[][] data) {
		int numRow=data.length; int numCol=data[0].length;
		DenseMatrix64F featMatrix=new DenseMatrix64F(numRow,numCol);
		for(int i=0; i<numRow;i++){
			for(int j=0; j<numCol;j++){
				featMatrix.set(i, j, data[i][j]); 
			}
		}
		return featMatrix;
	}
	
	public static double[] getOneRow_from_DenseMatrix(DenseMatrix64F featMatrix, int row_i) {
		double[] oneSimRow=new double[featMatrix.getNumCols()];
		for(int i=0; i<featMatrix.getNumCols();i++){
			oneSimRow[i]=featMatrix.get(row_i, i);
		}
		return oneSimRow;
	}

	public static String makeHEsignature_Str(double[] feat, DenseMatrix64F pMatrix, double[] HEThreshold, int RADIX) {//according to paper-HE, it is P*x
		DenseMatrix64F projectFeat=matrix_Mut(pMatrix,douArrToDenseMatrix(feat, false));	//1-coloumn vector	
		int HElength=projectFeat.numCols;
		StringBuffer HESig=new StringBuffer();
		for(int i=0; i<HElength;i++){
			if(projectFeat.get(i)>HEThreshold[i]){
				HESig.append("1");
			}else{
				HESig.append("0");
			}
		}
		BigInteger bigInt=new BigInteger(HESig.toString(),2);
		return bigInt.toString(RADIX); //Character.MAX_RADIX==36, save binary into number, can save data space
	}
	
	public static BitSet makeHEsignature_BitSet(double[] projectFeat, float[] HEThreshold) {
		int HElength=HEThreshold.length;
		BitSet HESig=new BitSet(HElength);
		for(int i=0; i<HElength;i++){
			if(projectFeat[i]>HEThreshold[i]){
				HESig.set(i);// set i-th == true 
			}
		}
		return HESig;
	}
	
	public static double[] projectFeat(DenseMatrix64F pMatrix, double[] feat){//according to paper-HE, it is P*feat: P_matrix * feat(1 column vector)
		DenseMatrix64F projectFeat=matrix_Mut(pMatrix, douArrToDenseMatrix(feat, false));//1 column vector	
		return projectFeat.data;
	}
	
	public static double[] projectFeat(DenseMatrix64F pMatrix, float[] feat){//according to paper-HE, it is P*feat: P_matrix * feat(1 column vector)
		DenseMatrix64F projectFeat=matrix_Mut(pMatrix, floatArrToDenseMatrix(feat, false));//1 column vector	
		return projectFeat.data;
	}
	
	public static DenseMatrix64F matrix_Mut(DenseMatrix64F A, DenseMatrix64F B) {//
		DenseMatrix64F projectFeat= new DenseMatrix64F(A.getNumRows(),B.getNumCols());
		CommonOps.mult(A,B,projectFeat);
		return projectFeat;
	}

	public static float cosSim(float[] a, float[] b) {
		DenseMatrix64F A=General_EJML.floatArrToDenseMatrix(a, false);
		DenseMatrix64F B=General_EJML.floatArrToDenseMatrix(b, false);
		DenseMatrix64F C=new DenseMatrix64F(1,1);
		CommonOps.multTransA(A,B, C);
		double cosSim=C.get(0)/NormOps.fastNormP2(A)/NormOps.fastNormP2(B);
		return (float) cosSim;
	}

}
