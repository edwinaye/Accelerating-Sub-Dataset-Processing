package approximationmeta.src;

import approximationmeta.bloomfilter.*;

import java.util.*;
import java.io.*;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;

import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.InputStreamReader;


import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.IOException;

public class PartitionStatistic implements Comparable<PartitionStatistic>{
	
	public String fileName;
	public String[] hosts;

	int partitionID;
	public int quantity;
	int cutoff; 
	int distance;
	int dominateSize;
	int bloomSize;
 
	HashMap<String, Integer> statisticMap;
	HashMap<String, Integer> fullMap;
	BloomFilter<String> bloomFilter;
	
	public PartitionStatistic(String file, int cut){
		fileName = file;
		cutoff = cut;
	}

	public void buildStatistic() {
	
	HashMap<String, Integer> map = new HashMap<String, Integer>();   
	//int[] interval = {1,4,7,12,20,33};
	int[] interval = new int[32];
	
	for(int i=0; i < 32;i++)
		interval[i] = i+1;

	int[] count = new int[interval.length];

	try{
		/*File file = new File(fileName);
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		String line;
		*/			
		String path = fileName;
		Configuration conf = new Configuration();
		Path coreSitePath = new Path(System.getenv("HADOOP_HOME"), "conf/core-site.xml");
		conf.addResource(coreSitePath);

		FileSystem hdfs = FileSystem.get(conf);			
		InputStream in  = hdfs.open(new Path(path));

  		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
		while((line = br.readLine()) != null) {
			String x = line.split(" ")[1];
			if(!map.containsKey(x)){
				map.put(x,1); count[0]++;}
			else{ 
				int val = map.get(x);
				map.put(x, val+1);
				if(val+1 > interval[count.length-1])continue;
				for(int i=count.length-1; i > 0; i--)
					if(val+1 == interval[i]) {count[i]++; break;}
			}
		}
		br.close();		
		in.close();		
	     } catch (IOException e) {
			e.printStackTrace();
	}

		fullMap = map;	

		int cutVal = interval[0], index =0, total = map.size();

		/*
		dominateSize = (int)cutoff * total > 1024? (int)cutoff*total : 1024;
			
		while(index < count.length && dominateSize < count[index]) index++;
	
		if(index >= count.length) {cutVal = interval[count.length-1];dominateSize =count[count.length-1];}
		else {cutVal = interval[index];dominateSize = count[index];}
		*/
		cutVal = cutoff;
		dominateSize = count[cutVal-1];
		double falsePositiveProbability = 0.01;
		
		bloomSize = total - dominateSize > 0? total-dominateSize:1;
	
		statisticMap = new HashMap<String, Integer>(dominateSize);
		bloomFilter = new BloomFilter<String>(falsePositiveProbability, bloomSize);
		
		bloomSize = 0;
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			String key = entry.getKey();
			int val = entry.getValue();
			if(val >= cutVal)
				statisticMap.put(key, val);
			else{
				bloomFilter.add(key); distance += (val-1); bloomSize++;
			}
		}
		dominateSize = statisticMap.size();
	}
				 
	public void printStatistic(){
		for(Map.Entry<String, Integer>  entry : statisticMap.entrySet())
			System.out.println(entry.getKey() +" " +entry.getValue());
	}

	public int lookup(String setName){
		if(statisticMap.containsKey(setName))
			quantity = statisticMap.get(setName);
		else if(bloomFilter.contains(setName))
			quantity = 1;
		else
			quantity = 0;

		return this.quantity;
	}

	public int fullLookup(String setName){
		if(fullMap.containsKey(setName))
			quantity = fullMap.get(setName);
		else
			quantity = 0;
		return this.quantity;
	}	

	public int compareTo(PartitionStatistic that){
		return that.quantity - this.quantity;
	}
}
