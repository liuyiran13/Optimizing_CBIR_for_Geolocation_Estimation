package MyAPI.General;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;


public class ComparableCls{
	/**
	 * to USe:
	 * 		
	 * Tips:
	 * 		
	 *
	 */
	
	/***
     * Compare the Floatt value to allow sorting in a tree map. If the Floatt value is the same, but the Intt
     * is different, the Intt is used to distinguishing the results. Otherwise the TreeMap
     * implementation wouldn't add the result.
     * @param o the slaveInt_masterFloat to compare the current one to.
     * @return -1, 0, or 1
     */
	@SuppressWarnings("rawtypes")
	public static class slave_masterFloat_DES <V extends Object> implements Comparable<slave_masterFloat_DES> {
		private V  slave;
		private float Floatt;
		public slave_masterFloat_DES(V  Intt, float Floatt) {
	        this.slave = Intt;
	        this.Floatt = Floatt;
	    }

		public V getSlave() {
	        return slave;
	    }
		
	    public float getMaster() {
	        return Floatt;
	    }

	    public void set_slave_master(V  Intt, float Floatt) {
	    	this.slave = Intt;
	        this.Floatt = Floatt;
	    }

	    @Override
	    public int compareTo(slave_masterFloat_DES obj) {
	        int compareValue = (int) Math.signum(this.Floatt - obj.Floatt);
	        if (compareValue==0) {//for remove method, it need exactly obj match! so same obj, same address, same hashcode!
	        	compareValue=this.hashCode()-obj.hashCode();
	        }
	        return compareValue;
	    }
	    
	    @Override
	    public String toString(){
	    	return "Sco"+Floatt+"_Sam"+slave.toString();
	    }

	    public static <K extends Object> void SetToList(NavigableSet<slave_masterFloat_DES<K>> sortedSet, List<K> topSamples, List<Float> topScores){
	    	for (slave_masterFloat_DES<K> slaveInt_masterFloat : sortedSet) {//in descending, default ascending order
				topSamples.add(slaveInt_masterFloat.getSlave());
				topScores.add(slaveInt_masterFloat.getMaster());
			}
	    }
	}
	
	@SuppressWarnings("rawtypes")
	public static class slave_masterFloat_ASC <V extends Object> implements Comparable<slave_masterFloat_ASC> {
		private V  slave;
		private float Floatt;
		public slave_masterFloat_ASC(V  Intt, float Floatt) {
	        this.slave = Intt;
	        this.Floatt = Floatt;
	    }

		public V getSlave() {
	        return slave;
	    }
		
	    public float getMaster() {
	        return Floatt;
	    }

	    public void set_slave_master(V  Intt, float Floatt) {
	    	this.slave = Intt;
	        this.Floatt = Floatt;
	    }

	    @Override
	    public int compareTo(slave_masterFloat_ASC obj) {
	        int compareValue = (int) Math.signum(obj.Floatt- this.Floatt);
	        if (compareValue==0) {//for remove method, it need exactly obj match! so same obj, same address, same hashcode!
	        	compareValue=this.hashCode()-obj.hashCode();
	        }
	        return compareValue;
	    }

	}

	public static class slave_masterInteger_DES <V extends Object> implements Comparable<slave_masterInteger_DES<V>> {
		private V  slave;
		private int integer;
		
		public slave_masterInteger_DES(V  Intt, int integer) {
	        this.slave = Intt;
	        this.integer = integer;
	    }

		public V getSlave() {
	        return slave;
	    }
		
	    public int getMaster() {
	        return integer;
	    }

	    public void set_slave_master(V  Intt, int integer) {
	    	this.slave = Intt;
	        this.integer = integer;
	    }

		@Override
	    public int compareTo(slave_masterInteger_DES<V> obj) {
	        int compareValue = (int) Math.signum(this.integer - obj.integer);
	        if (compareValue==0) {//for remove method, it need exactly obj match! so same obj, same address, same hashcode!
	        	compareValue=this.hashCode()-obj.hashCode();
	        }
	        return compareValue;
	    }
	    
	    @Override
	    public String toString(){
	    	return "Master:"+integer+", Slave:"+slave.toString();
	    }

	}
	
	public static class slave_masterInteger_ASC <V extends Object> implements Comparable<slave_masterInteger_ASC<V>> {
		private V  slave;
		private int integer;
		public slave_masterInteger_ASC(V  Intt, int integer) {
	        this.slave = Intt;
	        this.integer = integer;
	    }

		public V getSlave() {
	        return slave;
	    }
		
	    public int getMaster() {
	        return integer;
	    }

	    public void set_slave_master(V  Intt, int integer) {
	    	this.slave = Intt;
	        this.integer = integer;
	    }

		@Override
	    public int compareTo(slave_masterInteger_ASC<V> obj) {
	        int compareValue = (int) Math.signum(obj.integer-this.integer);
	        if (compareValue==0) {//for remove method, it need exactly obj match! so same obj, same address, same hashcode!
	        	compareValue=this.hashCode()-obj.hashCode();
	        }
	        return compareValue;
	    }
	    
	    @Override
	    public String toString(){
	    	return "Master:"+integer+", Slave:"+slave.toString();
	    }

	}
	
	public static void main(String[] args) throws IOException {//for debug!
		slave_masterFloat_DES<Integer> a=new slave_masterFloat_DES<Integer>(1,0);
		slave_masterFloat_DES<Integer> b=new slave_masterFloat_DES<Integer>(1,(float) 0.2);
		System.out.println(a.equals(b));
	}
}

