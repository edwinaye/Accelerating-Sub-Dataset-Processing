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
 
public class WriteToHDFS {

    public void writeback(double[][] score, LinkedList<String> subsets, FileSystem hdfs, String movie, String host){
	
	try{
	
		Path filenamePath = new Path("/movieOutput/"+movie+"with"+host);  
		FSDataOutputStream fin = hdfs.create(filenamePath);

		for(int i=0; i < score.length;i++)
			for(int j=0; j < score[i].length; j++)
				fin.writeDouble(score[i][j]);

		for(String s: subsets)
			fin.writeUTF(s);
	
		fin.close();

	} catch(IOException e) {
		e.printStackTrace();
	}
    }		
}
