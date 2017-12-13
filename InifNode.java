
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

/**
 * Created 11/7/2016
 * Software Development - Team 2063-1
 * Colorado TSA Conference - Feb 2017
 *
 * Purpose: InifNode defines the methods available
 * to a remote RMI request from the Node class. Thus,
 * the interface extends Remote, and each methods can
 * throw a RemoteException
 */
public interface InifNode extends Remote {

    void setShard(Shard shard) throws RemoteException;

    Shard getShard() throws RemoteException;

    void setArrayData(Array data) throws RemoteException;
    //Set Array that Node belongs no

    void startService() throws RemoteException;

    void unassignNode(String reason) throws RemoteException;

    void terminateNode(String reason) throws RemoteException;

    UUID getID() throws RemoteException;

}
