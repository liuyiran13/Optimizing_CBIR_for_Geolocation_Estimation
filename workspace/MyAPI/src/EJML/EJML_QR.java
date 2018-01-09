package EJML;

import java.util.Random;

import org.ejml.data.DenseMatrix64F;
import org.ejml.factory.DecompositionFactory;
import org.ejml.factory.QRDecomposition;
import org.ejml.ops.CommonOps;

public class EJML_QR {

	/**
	 * first try of Efficient Java Matrix Library
	 */
	public static void main(String[] args) {
		
		int numRow=64; int numCol=64;
		DenseMatrix64F matrix_toFac = new DenseMatrix64F(numRow,numCol) ; 
		Random rand=new Random();
		//Gaussian random generate A
		for(int i=0;i<numRow;i++){
			for(int j=0;j<numCol;j++){
				matrix_toFac.set(i, j, rand.nextGaussian());
			}
		}
		QRDecomposition<DenseMatrix64F> qr = DecompositionFactory.qr(matrix_toFac.numRows,matrix_toFac.numCols);
		if( !qr.decompose(matrix_toFac) )
            throw new RuntimeException("Decomposition failed");
		 
//		QR factorization have two model,  m×n matrix A, with m ≥ n,
//		1. non-compact: Q(m×m) R(m×n),  R: n×n uper triangular + the last numRow-numCol rows are all zeros
//		2. compact: Q1(m×n) R1(n×n), 

		boolean compact=true; 
		DenseMatrix64F matrix_Q=qr.getQ(null, compact);
		DenseMatrix64F matrix_R=qr.getR(null, compact);
		System.out.println("matrix_Q: row num:"+matrix_Q.getNumRows()+", coloumn num:"+matrix_Q.getNumCols());
		System.out.println("matrix_R: row num:"+matrix_R.getNumRows()+", coloumn num:"+matrix_R.getNumCols());
		
		DenseMatrix64F I =new DenseMatrix64F(matrix_Q.getNumRows(),matrix_Q.getNumRows());
		CommonOps.multTransB(matrix_Q,matrix_Q,I);
		I.print();
		
		
	}

}
