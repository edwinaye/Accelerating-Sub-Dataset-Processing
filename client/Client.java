package client;
 
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import communication.*;
import mapreducer.*;
import java.util.*;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class Client {

    private void startJob(String id){
        try {
		
		long initTime = System.currentTimeMillis();
		
		Registry myRegistry = LocateRegistry.getRegistry("cass", 1099);		
		Configuration conf = new Configuration();
		Path coreSitePath = new Path(System.getenv("HADOOP_HOME"), "conf/core-site.xml");
		conf.addResource(coreSitePath);	
		FileSystem hdfs = FileSystem.get(conf);	
		
		long assiTime = System.currentTimeMillis();
		Data impl = (Data) myRegistry.lookup("myMessage");		
		//String choice = impl.readSearch();

/*
	  	 while(true){
			String movieID = null;			
			while(true){
				movieID = impl.readTask();
				for(int i =0; i < 100; i++) ;
				if( movieID.equals(last)) continue;
				else break;
		}
			
*/		
		String	movieID = impl.readTask();
		LinkedList<String> taskList = new LinkedList<String>(impl.getAssign(id));
					
		long mapTime = System.currentTimeMillis();		
		LinkedList<String> subsets = new ClientMapper().mapper(hdfs, taskList, movieID);
		
		long reduceTime = System.currentTimeMillis();
		double[][] score = new ClientReducer().reducer(subsets);
		
		long networkTime = System.currentTimeMillis();
		new WriteToHDFS().writeback(score, subsets, hdfs, movieID, id);
	
		long it = assiTime - initTime, at = mapTime-assiTime, mt = reduceTime-mapTime, rt = networkTime - reduceTime;
		long wt = System.currentTimeMillis()-networkTime; 
		long all = System.currentTimeMillis()-initTime;

		double[] res = {taskList.size(), subsets.size(), it, at, mt, rt, wt, all};

		impl.checkOut(id, res);

	      String statistic = id + " maps= "+taskList.size()+" wl= " + subsets.size() + " it= " + it + " at= " + at + " mt= " +mt+ " rt= " +rt + " wt " + wt+"\n";
		System.out.println(statistic);
		//last = movieID;
	   

        } catch (Exception e) {
            e.printStackTrace();
        }       
    }

   public static void main(String[] args) {
        Client client = new Client();
	String id = args[0];
        client.startJob(id);
    }
}
