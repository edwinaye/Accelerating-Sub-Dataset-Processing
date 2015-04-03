package approximationsearch.src;

import java.io.*;
import approximationsearch.bloomfilter.*; 
import approximationsearch.app.*;
 
public class ASAP {
 
   public static void main (String[] args) {

 	if(args.length < 3)
		System.exit(1);

	String fileList = args[0], movieList = args[1];
	double cutoff = Double.parseDouble(args[2]);
	int flag = Integer.parseInt(args[3]);
	
	MetaServer server = new MetaServer(); 	 	
	server.buildFileList(fileList);
	server.buildMeta(cutoff);
	

	System.out.println("Enter your search movie id: ");
	BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	
	try{
		
		String movieID = br.readLine();
		while(!movieID.equals("q!")){
			server.lookDistribution(movieID);
			Count r0 = new Count("Thread-0", 0, 4);
			Count r1 = new Count("Thread-1", 1, 4);
			Count r2 = new Count("Thread-2", 2, 4);
			Count r3 = new Count("Thread-3", 3, 4);	
			r0.setTask(movieID, server, flag);
			r1.setTask(movieID, server, flag);
			r2.setTask(movieID, server, flag);
			r3.setTask(movieID, server, flag);
			r0.start();
			r1.start();
			r2.start();
			r3.start();
				
			movieID = br.readLine();
				
/*			r0.setTask(movieID, server, 1);
			r1.setTask(movieID, server, 1);
			r2.setTask(movieID, server, 1);
			r3.setTask(movieID, server, 1);	
			r0.start();
			r1.start();
			r2.start();
			r3.start();*/
		}
	
	}catch(IOException e){	
		e.printStackTrace();	
	}	
	/*
	try {
		File ml = new File(movieList);
		FileReader fr = new FileReader(ml);
		BufferedReader br = new BufferedReader(fr);
	
 		String movie = null;
		int distance = 0;
		int allcount = 0;
		while( (movie = br.readLine())!= null) {	
	      		String movieID = movie.split(":")[0];
			distance += server.distanceLookup(movieID);
			allcount += server.allCount(movieID); 
			System.out.println(server.checkDistribution(movieID));
			System.out.println(server.fullDistribution(movieID));
 		}
		System.out.println("distance = " + distance + " total= " + allcount);
		br.close();
		fr.close();
	}catch(IOException e){
		e.printStackTrace();	
	}
	*/

	System.out.println("Third memory="+Runtime.getRuntime().freeMemory());

     } 
} 
