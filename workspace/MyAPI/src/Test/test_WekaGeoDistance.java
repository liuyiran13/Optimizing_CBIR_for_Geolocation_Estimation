package Test;
import MyAPI.General.Weka_GeoDistance;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;


public class test_WekaGeoDistance {

	public static void main(String[] args) throws Exception{
		
		int attNum=2+1;//feat nums + 1 ( attribute for class name)
		String[] className={"cl_0","cl_1","cl_2"};
		//form attribute vector
		FastVector fvWekaAttributes = new FastVector(attNum);
			// Declare the class attribute along with its values
			FastVector fvClassVal = new FastVector(className.length);
			for(String clNam: className){
				fvClassVal.addElement(clNam);
			}
			Attribute ClassAttribute = new Attribute("theClass_"+(attNum-1), fvClassVal);
		for(int i=0;i<attNum-1;i++){
			fvWekaAttributes.addElement(new Attribute("Feat_"+i));    
		}
		fvWekaAttributes.addElement(ClassAttribute);// last attribute is class label

		// Create an empty Instances set
		Instances wekaInsts = new Instances("testInst", fvWekaAttributes, 10);   
		// Set class index
		wekaInsts.setClassIndex(attNum-1); // last attribute is class label, index starts with 0, If the class index is negative there is assumed to be no class. (ie. it is undefined)
 
		double[] GPSdata1={-33.764843,150.65878}; //lat + long
		Instance iInst1 = new Instance(attNum); iInst1.setValue((Attribute)fvWekaAttributes.elementAt(attNum-1), className[0]);
		for(int i=0;i<GPSdata1.length;i++){
			iInst1.setValue((Attribute)fvWekaAttributes.elementAt(i), GPSdata1[i]); //lat + long
		}
		wekaInsts.add(iInst1); //add one instance to wekaInsts
		
		double[] GPSdata2={50.040604,-5.652809}; //lat + long
		Instance iInst2 = new Instance(attNum); iInst2.setValue((Attribute)fvWekaAttributes.elementAt(attNum-1), className[0]);
		for(int i=0;i<GPSdata2.length;i++){
			iInst2.setValue((Attribute)fvWekaAttributes.elementAt(i), GPSdata2[i]); //lat + long
		}
		wekaInsts.add(iInst2); //add one instance to wekaInsts
		
		Weka_GeoDistance geoDist= new Weka_GeoDistance(wekaInsts); //distance in km
		String[] options={"-M","GreatCircle"};
		geoDist.setOptions(options);
		System.out.println("geoDist.distance(iInst1, iInst2): "+geoDist.distance(iInst1, iInst2));
		
	}
}
