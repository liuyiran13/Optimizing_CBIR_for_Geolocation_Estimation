package MyAPI.ConceptDetector;

public class svm_tools {
	
	public static svm_node[] data_sparse(float[] oneArray) {// tranfer from one array to sparse svm_node[]
	    int realFtNum=0; //array total feat number
	    for (int j = 0; j < oneArray.length; j++){
	    	if (oneArray[j]!=0)
	    		realFtNum++;
	    }	    
		svm_node[] nodeArray = new svm_node[realFtNum];
		int nodeIndex=0;
	    for (int j = 0; j < oneArray.length; j++){
	    	if (oneArray[j]!=0){
	    		nodeArray[nodeIndex] = new svm_node();
	    		nodeArray[nodeIndex].index = j;
	    		nodeArray[nodeIndex].value = oneArray[j];
	            nodeIndex++;
	    	}        
	    }    
	    return nodeArray;
	}


}
