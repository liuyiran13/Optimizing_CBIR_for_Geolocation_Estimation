package MyAPI.Obj;

import org.apache.hadoop.conf.Configuration;

public class Conf_General {
	public Configuration conf;
	public static final String hdfs_address="hdfs://hathi-surfsara/"+"user/yliu/"; //hdfs://head02.hathi.surfsara.nl/, hdfs://hathi-surfsara/
	
	public Conf_General(Configuration conf) {
		this.conf=conf;
	}

}
