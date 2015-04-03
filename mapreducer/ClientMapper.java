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
 
public class ClientMapper {

    public LinkedList<String> mapper(FileSystem hdfs, LinkedList<String> files, String movie){	
	
	LinkedList<String> reviews = new LinkedList<String>();
	
	try{
		InputStream in = null;
		for(String path : files ){
  			in = hdfs.open(new Path(path));
  			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			line = br.readLine();
			while(line != null){
				String[] tokens = line.split(" ");
				if(tokens[1].equals(movie))
					reviews.add(tokens[4]);
				line = br.readLine();
			}
	
			br.close();
		}
	}
	catch(IOException e){
		e.printStackTrace();
	}
	return reviews;
   }
}
