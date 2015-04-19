package scheduler;

import approximationmeta.src.*;
import java.util.*;

public class DistributionScheduler {

	public	HashMap<String, LinkedList<String>> defaultAssignment;
	public 	HashMap<String, LinkedList<String>> claspAssignment;
	
	public void printAssign(NodesToFile ntf, int flag){
		HashMap<String, LinkedList<String>> ass = null;
		if(flag == 0) ass = claspAssignment;
		if(flag == 1) ass = defaultAssignment;
		if(ass == null)
			return ;
		for(String cpt : ntf.computers){
			String s = cpt;
			for(String e : ass.get(cpt))
				s += e;
			System.out.println(s);
		}
	}

	public void balanceAssign()
	
	public void claspAssign(MetaServer ms, NodesToFile ntf){
		claspAssignment = new HashMap<String, LinkedList<String>>();		
		HashMap<String, FileWorkload> 	hfw = new HashMap<String, FileWorkload>();
		HashSet<String> set = new HashSet<String>();

		for(String s : ntf.computers){
			claspAssignment.put(s, new LinkedList<String>());
			hfw.put(s, new FileWorkload());
		}

		int count = 0;
		for(int i=0; i < ms.globalMap.length;i++)
			if(ms.globalMap[i].quantity != 0) count++;
		
		int average = count/claspAssignment.size();
		average = average*claspAssignment.size() < count? average+1 : average;

		for(int i =0; i < ms.globalMap.length && ms.globalMap[i].quantity != 0;i++){
			String[] hosts = ms.globalMap[i].hosts;
			String h = hosts[0];
			for(int j =1; j < hosts.length; j++){
				if(hfw.get(h) == null)
					h = hosts[j];
				else if (hfw.get(hosts[j])!= null && hfw.get(hosts[j]).files < average  && hfw.get(hosts[j]).workload < hfw.get(h).workload)
					h = hosts[j];
			}
			if(hfw.get(h) != null && hfw.get(h).files < average){
				hfw.get(h).files += 1;
				hfw.get(h).workload += ms.globalMap[i].quantity; 
				claspAssignment.get(h).add(ms.globalMap[i].fileName);
			}
			else
				set.add(ms.globalMap[i].fileName);
		}

		for(String file : set){
			for(String host : ntf.computers) 
				if(claspAssignment.get(host).size()<average)
					claspAssignment.get(host).add(file);
		}
			
	}
		
	public void defaultAssign(MetaServer ms, NodesToFile ntf){
		defaultAssignment = new HashMap<String, LinkedList<String>>();	
		HashSet<String> setFile = new HashSet<String>();	
		
		for(String s : ntf.computers)
			defaultAssignment.put(s, new LinkedList<String>());
		
		int totalfiles = ms.globalMap.length;
		int average = (int)totalfiles/ntf.computers.size();
		if(average * ntf.computers.size() < totalfiles)
			average +=1;

		for(int i =0; i < ms.globalMap.length;i++){
			String[] hosts = ms.globalMap[i].hosts;
			boolean ass = false;
			for(String host : hosts){					
				LinkedList<String> ll = defaultAssignment.get(host);
				if(ll != null && ll.size() < average){
					ll.add(ms.globalMap[i].fileName); ass = true; break;
				}		
			}
			if(ass == false) 
				setFile.add(ms.globalMap[i].fileName);		
		}
		
		for(String e: setFile){
			for(String host : ntf.computers){
				LinkedList<String> ll = defaultAssignment.get(host);
				if(ll.size()<average) ll.add(e);
			}
		}
	}

}


