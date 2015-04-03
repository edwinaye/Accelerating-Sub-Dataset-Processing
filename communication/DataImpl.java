package communication;

import scheduler.*; 
 
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class DataImpl extends UnicastRemoteObject implements Data, Serializable {

	public String files;
	public DistributionScheduler ds;
	public String movieID; 
	public String scheChoice;

	public DataImpl() throws RemoteException {
	}
	
	@Override
	public LinkedList<String> getAssign(String hostid, String flag) throws RemoteException {
		if(flag.equals("0"))
			return ds.claspAssignment.get(hostid);
		else
			return ds.defaultAssignment.get(hostid);
	}

    	@Override 
	public String readTask() throws RemoteException {
		return movieID;
	}

	@Override
	public String readSearch() throws RemoteException {
		return scheChoice;
	}	

	@Override
	public void sayHello(String name) throws RemoteException {
        	for(int i =0; i < 100000; i++)
			System.out.println("hello from "+ name + " i= "+ i);

    	}

	@Override     
	public int taskExe(LinkedList<String> files, String movie) throws Exception{
		//String path = file;
		int count =0;
		Configuration conf = new Configuration();
		Path coreSitePath = new Path(System.getenv("HADOOP_HOME"), "conf/core-site.xml");
		conf.addResource(coreSitePath);

		FileSystem hdfs = FileSystem.get(conf);			
		InputStream in = null;
		try {
			for(String path : files ){
  				in = hdfs.open(new Path(path));
  				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String line;
				line = br.readLine();
				while(line != null){
					if(line.split(" ")[1].equals(movie)){
						System.out.println(line); count++;}
					line = br.readLine();
				}
			}
		} finally {
  			IOUtils.closeStream(in);
		}
		return count;
	}	
}

