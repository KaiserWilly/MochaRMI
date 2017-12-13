

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

/**
 * Created 11/17/2016
 * Software Development - Team 2063-1
 * Colorado TSA Conference - Feb 2017
 *
 * Purpose: InifServerClient defines the methods available
 * to a remote RMI request from the Server side
 * of the Query class. Thus, the interface extends Remote,
 * and each method can throw a RemoteException
 */
public interface InifQueryServer extends Remote {
    Node registerNode(Node n) throws RemoteException;
    //Register connection with Node

    void removeArray(Array a) throws RemoteException;
    //Dissolve Array

    void printUnassignedNodes() throws RemoteException;
    //Console print Unassigned nodes on Query server

    ArrayList<Shard> getShardList() throws RemoteException;

    ArrayList<Array> getArrayList() throws RemoteException;

    ArrayList<Node> getUnassignedNodes() throws RemoteException;

    void stopQuery(String altQryIP, int altQryPrt) throws RemoteException;
    //Stop query server, designating an alternate to transfer arrayMeta

    void stopQuery(String reason) throws RemoteException;
}
