
package communication;
 
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.*;
 
public interface Data extends Remote {
 	void clearResults() throws RemoteException;
 	int taskExe(LinkedList<String> files, String movieID) throws Exception;
	LinkedList<String> getAssign(String hostName) throws RemoteException; 
	String readTask() throws RemoteException;
	HashMap<String, double[]> getResults() throws RemoteException;
	int checkResults() throws RemoteException; 
	String readSearch() throws RemoteException;
	void checkOut(String id, double[] result) throws RemoteException;
}

