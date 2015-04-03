
package server;
 
import communication.*;
import approximationmeta.src.*;
import scheduler.*;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.*;
 
public class Server {
     
    private void startServer(MetaServer ms, NodesToFile ntf){
        try {
		Registry registry = LocateRegistry.createRegistry(1099);
		DataImpl share = new DataImpl();
		
		DistributionScheduler ds = new DistributionScheduler();
		ds.defaultAssign(ms, ntf);

		share.ds = ds;
		registry.rebind("myMessage", share);
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));				
			String line = br.readLine();
			while(!line.equals("q!")){
				String[] token = line.split("-"); 
				share.movieID = token[0];
				share.scheChoice = token[1];
				int scheChoice = Integer.parseInt(token[1]);				
				
				if(scheChoice == 0){					
					ms.lookDistribution(token[0]);
					ds.claspAssign(ms, ntf);	
				}
				//ds.printAssign(ntf);
				
				line = br.readLine();
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		//System.out.println("fileName= " + share.files);
        } catch (Exception e) {
            e.printStackTrace();
        }     
        System.out.println("system is ready");
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
