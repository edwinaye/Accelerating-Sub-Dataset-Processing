package approximationmeta.src;

import java.util.*;
import java.io.*;
import approximationmeta.bloomfilter.*; 
import approximationmeta.app.*;
import approximationmeta.scheduler.*;
import java.util.concurrent.CyclicBarrier;
 
public class ASAPwithHDFS {
 
	MetaServer server;
	String fileList;
	String movieList;
	int cutoff;
	int flag;
	// Scheduler sch;

public ASAPwithHDFS(String[] args){

 	if(args.length < 3)
		System.exit(1);

	fileList = args[0];
	movieList = args[1];
	cutoff = Integer.parseInt(args[2]);
	flag = Integer.parseInt(args[3]);
	
	server = new MetaServer(); 	 	
	server.buildFileList(fileList);
	server.buildMeta(cutoff);
	
	//Scheduler sch = new Scheduler();
    
     } 
} 
