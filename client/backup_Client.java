package client;
 
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import transfer.*;
import java.util.*;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class Client {

    private int mapperCombiner(LinkedList<String> files, String movie, String host){
	int count =0;
	long first = System.currentTimeMillis();
	Configuration conf = new Configuration();
	Path coreSitePath = new Path(System.getenv("HADOOP_HOME"), "conf/core-site.xml");
	conf.addResource(coreSitePath);
	
	try{
	FileSystem hdfs = FileSystem.get(conf);			
	InputStream in = null;
	Path filenamePath = new Path("/movieOutput/"+movie+"with"+host);  
	FSDataOutputStream fin = hdfs.create(filenamePath);

	long second = System.currentTimeMillis();
	//long firstend = second - firstPhase;

	LinkedList<String> reviews = new LinkedList<String>();
 
	for(String path : files ){
  		in = hdfs.open(new Path(path));
  		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
		line = br.readLine();
		while(line != null){
			String[] tokens = line.split(" ");
			if(tokens[1].equals(movie)){
				//fin.writeUTF(line); count++;
				reviews.add(tokens[4]); count++;
				
					//System.out.println(line); count++;
			}
				line = br.readLine();
		}
			br.close();
		}
		//fin.close();
	long third = System.currentTimeMillis();
	
	/*
	for(String s : reviews){
		char[] rwc = s.toCharArray();
		Arrays.sort(rwc);
		String ss = new String(rwc);
	}
		
	Collections.sort(reviews);
	*/
	
	for(int i=0; i < reviews.size();i++)
		for(int j=0; j < reviews.size(); j++){
			int score = editDistance(reviews.get(i),reviews.get(j));
	}

	for(String s: reviews)
		fin.writeUTF(s);
	fin.close();

	long fourth = System.currentTimeMillis();
	long f1 = second-first, f2 = third-second, f3 = fourth - third;
	System.out.println("files="+files.size() + " wl= "+count+" hdfsInit= " + f1+" mapper= " + f2 + " localSort= " + f3);
	} catch(IOException e) {
		e.printStackTrace();
  		//IOUtils.closeStream(in);		
	}
		return count;
	}	

public int editDistance(String s1, String s2) {
    s1 = s1.toLowerCase();
    s2 = s2.toLowerCase();

    int[] costs = new int[s2.length() + 1];
    for (int i = 0; i <= s1.length(); i++) {
      int lastValue = i;
      for (int j = 0; j <= s2.length(); j++) {
        if (i == 0)
          costs[j] = j;
        else {
          if (j > 0) {
            int newValue = costs[j - 1];
            if (s1.charAt(i - 1) != s2.charAt(j - 1))
              newValue = Math.min(Math.min(newValue, lastValue),
                  costs[j]) + 1;
            costs[j - 1] = lastValue;
            lastValue = newValue;
          }
        }
      }
      if (i > 0)
        costs[s2.length()] = lastValue;
    }
    return costs[s2.length()];
}	
	
    private void start(String id){
        try {
		//long begin = System.currentTimeMillis();
		Registry myRegistry = LocateRegistry.getRegistry("cass", 1099);
		Data impl = (Data) myRegistry.lookup("myMessage");
		String choice = impl.readSearch();
		LinkedList<String> tasks = new LinkedList<String>(impl.getAssign(id, choice));
		String movieID = impl.readTask();
		
		/*for(String task : tasks){
			System.out.println(id + " : " + task);
		}*/
		
		long begin = System.currentTimeMillis();

		int count = mapperCombiner(tasks, movieID, id);
		long duration = System.currentTimeMillis()-begin;

		//System.out.println(id + "files="+tasks.size()+" workload= " + count +" usedTime= " + duration);
        } catch (Exception e) {
            e.printStackTrace();
        }       
    }

   public static void main(String[] args) {
        Client client = new Client();
	String id = args[0];
        client.start(id);
    }
}
