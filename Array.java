
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created 10/24/2016
 * Software Development - Team 2063-1
 * Colorado TSA Conference - Feb 2017
 *
 * Purpose: Array defines a collection of Nodes that represent a single
 * entity. The array disperses roles and work across each node as directed
 * by the developer. This class allows for easy referencing of many arrayMeta
 * fields, and gives a friendly way to access remote machines.
 */
public class Array implements InifArray, Serializable {
    private ArrayList<Node> nodeList = new ArrayList(); //List of Nodes in Array
    private HashMap<String, Node> shardMap = new HashMap<>(); //Tying Nodes to Shards (Roles)
    private Node coreNode = null;
    private String queryIP;
    private int queryPort;

    public void addNode(Node node) throws RemoteException { //Add Node to Array
        nodeList.add(node);
    }

    public void removeNode(Node n) { //Remove Node from Array
        nodeList.remove(n);
    }

    public void setCoreNode(Node n) { //Set Core Node
        coreNode = n;
    }

    public Node getCoreNode() { //Retrieve Core Node arrayMeta
        return coreNode;
    }

    public void addShardMap(Node node) throws RemoteException { //Add link between Node and Shard in Array
        shardMap.put(node.getShard().getRole(), node);
    }

    public HashMap<String, Node> getShardMap() throws RemoteException { //Retrieve ShardMap
        return shardMap;
    }

    public ArrayList<Node> getNodeList() { //Retrieve NodeList
        return nodeList;
    }

    public void setQueryIP(String ip) { //Set IP of Query Server
        queryIP = ip;
    }

    public String getQueryIP() { //Retrieve Query server IP
        return queryIP;
    }

    public void setQueryPort(int port) { //Set port of Query server
        queryPort = port;
    }

    public int getQueryPort() { //Retrieve port of Query server
        return queryPort;
    }
}
