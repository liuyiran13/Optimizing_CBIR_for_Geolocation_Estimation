package HammingEmbedding;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import org.ejml.data.DenseMatrix64F;
import org.ejml.factory.DecompositionFactory;
import org.ejml.factory.QRDecomposition;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;

public class makeProjectionMatrix {

	/**
	 * make projection matrix P for hammming embedding
	 * 
	 * projection matrix should only run 1 time! the one used for caculating HammingThr and HammingSig shoud be the same!
	 * 
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException {
		String base_Path="O:/ImageRetrieval/SIFTVW/"; //O:/ImageRetrieval/SURFVW/, O:/ImageRetrieval/SIFTVW/
		//*********** make projection matrix in HammingEnbedding ***************
		int featDim=128;//64, 128
		int HEBitNum=128; 
		DenseMatrix64F P=General_BoofCV.make_HM_ProjectionMatrix(HEBitNum,featDim);
		System.out.println("done! P:"+P.numRows+"_"+P.numCols);
		System.out.println(P.toString());
		String filePath=base_Path+"HE_ProjectionMatrix"+HEBitNum+"-"+featDim;
		if (new File(filePath).exists()) {
			throw new InterruptedException(filePath+" arleay exist! projection matrix should only run 1 time! the one used for caculating HammingThr and HammingSig shoud be the same!");
		}else {
			General.writeObject(filePath, P);
		}
	}
	
	public static void oldCode() throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException{
		int numRow=64; int numCol=64;
		DenseMatrix64F matrix_toFac = new DenseMatrix64F(numRow,numCol) ; 
		Random rand=new Random();
		//Gaussian random generate 
		for(int i=0;i<numRow;i++){
			for(int j=0;j<numCol;j++){
				matrix_toFac.set(i, j, rand.nextGaussian());
			}
		}
		
		//QR factorization
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
		System.out.println(matrix_Q.get(0, 10));
		System.out.println("matrix_R: row num:"+matrix_R.getNumRows()+", coloumn num:"+matrix_R.getNumCols());
		
		//make projection matrix P
		String base_Path="D:\\xinchaoli\\Desktop\\My research\\My Code\\DataSaved\\ICMR2013\\SURFVW\\";

		DenseMatrix64F P=matrix_Q;
		General.writeObject(base_Path+"HE_ProjectionMatrix64", P);
		
		//test
		DenseMatrix64F P_read=(DenseMatrix64F) General.readObject(base_Path+"HE_ProjectionMatrix");
		System.out.println("P_read: row num:"+P_read.getNumRows()+", coloumn num:"+P_read.getNumCols());
		System.out.println(P_read.get(0, 10));
	}

}
