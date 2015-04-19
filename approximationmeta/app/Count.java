package approximationmeta.app;

import approximationsearch.src.*;
import java.util.*;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;

import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.InputStreamReader;

import java.io.IOException;

public class Count extends Thread{
	private Thread t;
	private String threadName;
	private int id, total;
	private LinkedList<String> files;	
	private String movie;
	private MetaServer meta;
	public long time;

	public Count(String name, int ID) {
		threadName = name;
		id = ID;
		//System.out.println("Creating "+ threadName);
	}
	public void setTask(String movieID, LinkedList<String> files){
		movie = movieID;
		this.files = files;
	}
	
	public long getTime(){
		return time;
	}
	public void run() {
			int len = files.size();
			int start = 0;
			int count = 0;
			long startTime = System.currentTimeMillis();
			while(start < len){
				try {	
					long t = System.currentTimeMillis();
					String file = files.get(start);
		        		FileReader fr = new FileReader(new File(file));	
					BufferedReader br = new BufferedReader(fr);
					String line;
			
					while((line = br.readLine()) != null) {
						String x = line.split(" ")[1];
						if(x.equals(movie)) {
							int k = 0; count++;
							while(k++ < 4);
						}
					}
					start += 1;
					br.close(); fr.close();
					long e = System.currentTimeMillis() - t;
					//System.out.println("id= " + id + " index= " + start + " time= " + e);
				} catch(IOException e){
					e.printStackTrace();
				}	
			}
				
			long use = System.currentTimeMillis()-startTime;
			System.out.println( "Time of thread " + id + " : " + use + " workload= " + count + " #ofFiles= " + len);
			time = use;
       }
	
		
	public void start (){
		//System.out.println("Starting " + threadName);
		if(t == null){
			t = new Thread (this, threadName);
			t.start();
		} 
	}
	
}
