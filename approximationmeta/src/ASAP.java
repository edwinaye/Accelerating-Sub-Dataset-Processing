package approximationmeta.src;

import java.util.*;
import java.io.*;
import approximationsearch.bloomfilter.*; 
import approximationsearch.app.*;
import approximationsearch.scheduler.*;
import java.util.concurrent.CyclicBarrier;
 
public class ASAP {
 
   public static void main (String[] args) {

 	if(args.length < 3)
		System.exit(1);

	String fileList = args[0], movieList = args[1];
	int cutoff = Integer.parseInt(args[2]);
	int flag = Integer.parseInt(args[3]);
	
	MetaServer server = new MetaServer(); 	 	
	server.buildFileList(fileList);
	server.buildMeta(cutoff);
//	server.calculate(cutoff);
/*
	System.out.println("Enter your search movie id: ");
	//BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	
	try{
		
		
		File ml = new File(movieList);
		FileReader fr = new FileReader(ml);
		BufferedReader br = new BufferedReader(fr);
	
	//	String movieID = br.readLine();
		Scheduler sch = new Scheduler();

    
		Count r0 = new Count("Thread-0", 0);
		Count r1 = new Count("Thread-1", 1);
		Count r2 = new Count("Thread-2", 2);
		Count r3 = new Count("Thread-3", 3);	
		

		//while(!movieID.equals("q!")){
		String movie = br.readLine();	
		while( movie != null) {	
	      	
			String movieID = movie.split(":")[0];
		
		//	CyclicBarrier barrier = new CyclicBarrier(4);
			server.lookDistribution(movieID);
			LinkedList<String> f0, f1, f2, f3;
			f0 = sch.taskSplit(server, flag, 0, 4, movieID);			
			f1 = sch.taskSplit(server, flag, 1, 4, movieID);			
			f2 = sch.taskSplit(server, flag, 2, 4, movieID);			
			f3 = sch.taskSplit(server, flag, 3, 4, movieID);			
	
			r0.setTask(movieID, f0);
			r1.setTask(movieID, f1);
			r2.setTask(movieID, f2);
			r3.setTask(movieID, f3);
			
			r0.run();
			r1.run();
			r2.run();
			r3.run();
		
			//barrier.await();	
			
			try {
    				Thread.sleep(25000);
			} catch (InterruptedException e) {
   				 e.printStackTrace();
         		}
	
			long t = Math.max(r0.getTime(), Math.max(r1.getTime(), Math.max(r2.getTime(), r3.getTime())));
			System.out.println("movieID= "+movieID+" MaxTime = " + t);
			
			movie = br.readLine();
			
			//movieID = br.readLine();	
		}
	
	}catch(IOException e){	
		e.printStackTrace();	
	}*/ 

	Accuracy[] smooth = new Accuracy[10680];
	int index =0; 
	try {
		File ml = new File(movieList);
		FileReader fr = new FileReader(ml);
		BufferedReader br = new BufferedReader(fr);
	
 		String movie = null;
		int distance = 0;
		int allcount = 0;
		while( index < smooth.length && (movie = br.readLine())!= null) {	
	      		String movieID = movie.split(":")[0];
			smooth[index] = new Accuracy();
			smooth[index].distance = server.distanceLookup(movieID);
			smooth[index].total = server.allCount(movieID); 
			index++;
			//System.out.println("movie= " + movieID + " distance = " + distance + " total= " + allcount);
			//System.out.println(server.checkDistribution(movieID));
			//System.out.println(server.fullDistribution(movieID));
 		}
//		System.out.println("movie= " + movie + " distance = " + distance + " total= " + allcount);
		br.close();
		fr.close();
	}catch(IOException e){
		e.printStackTrace();	
	}
	
	System.out.println("index=" + index + " ?=" + smooth.length);
	Arrays.sort(smooth);

	for(int i =0; i < smooth.length; ){
		
		double reviews =0, distance =0;
		for(int j= i; j < i+100; j++){
			reviews += smooth[j].total;
			distance += smooth[j].distance;
		}
		
		System.out.println("movie from " + i + "to" + i+99 + " distance = " + distance + " totalReviews= " + reviews);
		
		i = Math.min(i + 100, smooth.length-1);
	}

	//System.out.println("Third memory="+Runtime.getRuntime().freeMemory());

     }

} 
