
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;

/**
 * Created 10/25/2016
 * Software Development - Team 2063-1
 * Colorado TSA Conference - Feb 2017
 *
 * Purpose: InifArray defines the methods available
 * to a remote RMI request from the Array class. Thus,
 * the interface extends Remote, and each methods can
 * throw a RemoteException
 */
public interface InifArray extends Remote {

    void setCoreNode(Node n) throws RemoteException;

    void addShardMap(Node node) throws RemoteException;

    HashMap<String, Node> getShardMap() throws RemoteException;
    // Get Map linking nodes to Shards
}
