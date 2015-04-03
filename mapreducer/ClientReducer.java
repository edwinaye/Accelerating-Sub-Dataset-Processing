package mapreducer;
 
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class ClientReducer {

public double[][] reducer(LinkedList<String> subsets){
	
	int len = subsets.size();
	double[][] score = new double[len][len];

	for(int i=0; i < len;i++)
		for(int j=0; j < len; j++)
			score[i][j] = editDistance(subsets.get(i),subsets.get(j));
	 return score;
}	

private int editDistance(String s1, String s2) {
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
              					newValue = Math.min(Math.min(newValue, lastValue), costs[j]) + 1;
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
	
}
