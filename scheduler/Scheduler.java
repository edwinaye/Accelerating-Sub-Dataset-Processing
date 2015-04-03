
package approximationmeta.scheduler;

import approximationsearch.src.*;
import java.util.*;

public class Scheduler {
	
	public LinkedList<String> taskSplit(MetaServer server, int flag, int id, int total, String movie){
		LinkedList<String> files = new LinkedList<String>();
		if(flag == 1)
			files = server.files;
		else {
			for(int i=0; i < server.globalMap.length; i++){
				if(server.globalMap[i].lookup(movie) !=0)
					files.add(server.globalMap[i].fileName);
			}
		}
		if(flag ==1)
			return defaultSplit(id, total, files);
		else
			return balanceSplit(id, total, files);
	}

	private LinkedList<String> defaultSplit(int id, int total, LinkedList<String> files){
		int len = files.size();
		int average = (len+1)/total;
		int start = id * average;
		LinkedList<String> res = new LinkedList<String>();
		while(start < (id+1)*average && start < len){
			res.add(files.get(start)); start++;
		}
		return res;
	}

	private LinkedList<String> balanceSplit(int id, int total, LinkedList<String> files){
		LinkedList<String> res = new LinkedList<String>();
		while(id < files.size()){
			res.add(files.get(id));
			id += total;
		}
		return res;
	} 
}
