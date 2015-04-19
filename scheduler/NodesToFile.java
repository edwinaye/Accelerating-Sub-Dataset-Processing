package scheduler;

import java.util.*;
import java.io.*;
import approximationmeta.src.*;

public class NodesToFile{
	HashMap<String, PriorityQueue<FileQuantity>> map;
	HashMap<String, LinkedList<String>> localMap; 

	public LinkedList<String> computers;
	
	public void buildComputers(String file){
		computers = new LinkedList<String>();
		try{
			File f = new File(file);
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while((line = br.readLine())!= null) {
				computers.add(line);
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	public void buildMap(MetaServer server){
		map = new HashMap<String, PriorityQueue<FileQuantity>>();
		localMap = new HashMap<String, LinkedList<String>>();

		for(String s : computers){
			PriorityQueue<FileQuantity> pq = new PriorityQueue<FileQuantity>();
			map.put(s, pq);
			
			localMap.put(s, new LinkedList<String>()); 
		}

		for(int i =0; i < server.globalMap.length; i++){
			String file = server.globalMap[i].fileName;
			int quan = server.globalMap[i].quantity;
			for(String host : server.globalMap[i].hosts){
				PriorityQueue<FileQuantity> pq = map.get(host);
				pq.add( new FileQuantity(file,quan));
				
				LinkedList<String> ll = localMap.get(host);
				ll.add(file);
			}
		} 
	}

	public void printMap(){
		for(String cpt : computers){
			String s = cpt;
			PriorityQueue<FileQuantity> pq = map.get(cpt);
			for(FileQuantity e : pq)
				s = s + e.fileName + "--" + e.quantity;

			System.out.println(s);
		}
	}
}
