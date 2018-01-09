package MyAPI.Obj;

import org.apache.hadoop.conf.Configuration;

public class Conf_localMachine {
	public Configuration conf;
	
	public Conf_localMachine() {
		//set conf
		this.conf=new Configuration();
		//set log4j logging system
		org.apache.log4j.BasicConfigurator.configure();//use the basic one, print to console
	}

}
