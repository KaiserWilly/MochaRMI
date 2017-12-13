

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created 12/7/2016
 * Software Development - Team 2063-1
 * Colorado TSA Conference - Feb 2017
 *
 * Purpose: InifQueryClient defines the methods available
 * to a remote RMI request from the Client side server
 * of the Query class. Thus, the interface extends Remote,
 * and each method can throw a RemoteException
 */
public interface InifQueryClient extends Remote {
    Array assignToArray(Client c) throws RemoteException;
    //Assign client to Array

    void openArray(Array data) throws RemoteException;
    //Make Array available to connection

    void closeArray(Array data) throws RemoteException;
    //Close Array from connections
}
