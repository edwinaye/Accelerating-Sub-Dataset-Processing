
package server;
 
import communication.*;
import approximationmeta.src.*;
import scheduler.*;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.*;
import java.util.*;
 
public class Server {
    private void executionSummary(HashMap<String, double[]> res, int size, String flag) {
	//double [][] sum = new double[res.size()][];
	double[][] summary = new double[3][size];

	for(Map.Entry<String, double[]> entry : res.entrySet()){
		double[] metrics= entry.getValue();
		for(int i =0; i < metrics.length; i++) {
			summary[0][i] = summary[0][i] ==0? metrics[i] : Math.min(summary[0][i], metrics[i]);
			summary[1][i] +=metrics[i];
			summary[2][i] = Math.max(summary[2][i], metrics[i]);	
		}
	}
	for(int i =0; i < summary[1].length; i++)
		summary[1][i] = summary[1][i]/res.size();

	for(int i =0; i < 3; i++) {
		String out = flag + " ";
		for (double x : summary[i]) out = out + " " +x;
		System.out.println(out);
	}
    } 


    private void startServer(MetaServer ms, NodesToFile ntf){
        try {
		Registry registry = LocateRegistry.createRegistry(1099);
		DataImpl share = new DataImpl();
		
		DistributionScheduler ds = new DistributionScheduler();
		share.ds = ds;
		registry.rebind("myMessage", share);
	
		   try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));				
			String line = br.readLine();
			while(!line.equals("q!")){
				String[] token = line.split("-"); 
				share.movieID = token[0];
				System.out.println("movie = " + share.movieID);
				//share.scheChoice = token[1];
				//int scheChoice = Integer.parseInt(token[1]);				
				
				ds.defaultAssign(ms, ntf.computers);
	
				share.clearResults();
				while(true) {
					for(int i=0; i < 100; i++) ;
					//System.out.println(share.checkResults());
					if(share.checkResults() == ntf.computers.size()){
						executionSummary(share.getResults(), 10, "1");
						break;
					}
				}
				
				share.clearResults();
				ds.balanceAssign( ntf.computers, ms, token[0]);
				while(true) {
					for(int i=0; i < 100; i++) ;
					if(share.checkResults() == ntf.computers.size()){
						executionSummary(share.getResults(), 10, "0");
						break;
					}
				}	
				
				line = br.readLine();
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		//System.out.println("fileName= " + share.files);
        } catch (Exception e) {
            e.printStackTrace();
        }     
    }

    public static void main(String[] args) {
	
	//ASAPwithHDFS meta = new ASAPwithHDFS(args);
	
	if(args.length < 4)
		System.exit(1);

	String fileList = args[0];
	String movieList = args[1];
	int cutoff = Integer.parseInt(args[2]);
	int flag = Integer.parseInt(args[3]);
	String computers = args[4]; 	
	
	
	//String movieID = args[5]; 

	MetaServer ms = new MetaServer(); 	 	
	ms.buildFileList(fileList);
	ms.buildMeta(cutoff);
	//System.out.println(ms.checkDistribution(movieID));
	// ms.lookDistribution("1");
	
	NodesToFile ntf = new NodesToFile();
	ntf.buildComputers(computers);
	
	//DistributionScheduler ds = new DistributionScheduler();
	//ds.makeAssign(ms, ntf, 1);
	//ds.printAssign(ntf);
	
        Server server = new Server();
        server.startServer(ms, ntf);
    }
}
