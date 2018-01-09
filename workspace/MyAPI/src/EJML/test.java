package EJML;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;

import MyAPI.General.General;
import MyAPI.General.General_EJML;

public class test {

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		
		//test General.douArrToDenseMatrix, General.DouArrToString
		int dim=64;
		double[] feat=new double[dim];
		Random rand=new Random();
		for(int i=0;i<dim;i++){
			feat[i]=rand.nextGaussian();
		}
		Arrays.sort(feat);
		DenseMatrix64F featMatrix= General_EJML.douArrToDenseMatrix(feat,false); // 1-vector
		System.out.println("featMatrix: row num:"+featMatrix.getNumRows()+", coloumn num:"+featMatrix.getNumCols());
		System.out.println(General.douArrToString(featMatrix.data, " ", "0.000"));
		
		// compute project feat
		String base_Path="D:\\xinchaoli\\Desktop\\My research\\My Code\\DataSaved\\ICMR2013\\SURFVW\\";

		DenseMatrix64F pMatrix=(DenseMatrix64F) General.readObject(base_Path+"HE_ProjectionMatrix");
		System.out.println("pMatrix: row num:"+pMatrix.getNumRows()+", coloumn num:"+pMatrix.getNumCols());

		DenseMatrix64F projectFeat= new DenseMatrix64F(pMatrix.getNumRows(),featMatrix.getNumCols());
		CommonOps.mult(pMatrix,featMatrix,projectFeat);
		System.out.println("projectFeat: row num:"+projectFeat.getNumRows()+", coloumn num:"+projectFeat.getNumCols());
		System.out.println(General.douArrToString(projectFeat.data, " ", "0.000"));
		
	}

}
