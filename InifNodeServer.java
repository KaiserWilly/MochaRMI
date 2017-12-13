
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created 11/17/2016
 * Software Development - Team 2063-1
 * Colorado TSA Conference - Feb 2017
 *
 * Purpose: InifNodeServer defines the methods available
 * to a remote RMI request from the Server side
 * of the Node class. Thus, the interface extends Remote,
 * and each method can throw a RemoteException
 */
public interface InifNodeServer extends Remote {
    boolean ping() throws RemoteException;
}
