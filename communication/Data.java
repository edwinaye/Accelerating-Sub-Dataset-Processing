
package communication;
 
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.*;
 
public interface Data extends Remote {
 	void sayHello(String name) throws RemoteException;
 	int taskExe(LinkedList<String> files, String movieID) throws Exception;
	LinkedList<String> getAssign(String hostName, String flag) throws RemoteException; 
	String readTask() throws RemoteException; 
	String readSearch() throws RemoteException;
}

