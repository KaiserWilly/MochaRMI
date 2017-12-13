

import java.net.Inet4Address;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

/**
 * MochaRMI - Decentralized Java RMI Framework
 * (c) JD Isenhart
 * Updated October 2017
 * <p>
 * QueryServer is the entity responsible
 * for managing the Nodes and Arrays that
 * make up the server. It organizes and
 * dispatches Nodes,
 */
public class QueryServer implements InifQueryServer {
    private HashMap<UUID, Node> nodeList = new HashMap<>();                 //List of all unorganized Nodes
    private ArrayList<Array> arrayList = new ArrayList<>();                 //List of all current Arrays
    private final ArrayList<Shard> SHARDS;                                  //List of Shards provided by Query
    private final Query QUERY;                                              //Query Metadata

    QueryServer(Query query) {
        this.QUERY = query;
        this.SHARDS = query.getShardList();
    }

    /**
     * registerNode
     * Called by the incoming Node, adds Node to registry
     * Updates registry if Node was present previously
     * Returns Query metadata back to the Node.
     */
     public synchronized Node registerNode(Node n) throws RemoteException {                   //Register new Node or Register free Node, called by Node
        UUID nodeID = n.getID();
        if (nodeID == null || !nodeList.keySet().contains(nodeID)) {
            nodeID = UUID.randomUUID();
            n = new Node(QUERY, nodeID);
            nodeList.put(nodeID,n);
        } else {
            nodeList.replace(nodeID, n);
        }
        if (nodeList.size() >= SHARDS.size()) {                                                     //If there is enough Shards to complete an array, create a new one.
            System.out.println("Creating new Array!");
            new Thread(new ArrayCreate()).start();
        }
        return n;         //Return Query metadata to Node
    }

    /**
     * checkoutNodes
     * Returns a collection of unused Nodes,
     * Amount of node specified as a parameter
     * Removes the Nodes from the master list
     */
    private synchronized ArrayList<Node> checkoutNodes(int numONodes) {
        if (numONodes > this.nodeList.size()) return null;
        try {

            ArrayList<Node> nodeL = new ArrayList<>();
            nodeL.addAll(((List<Node>) this.nodeList.values()).subList(0, numONodes - 1));
            for (Node n : nodeL) {
                nodeList.remove(n.getID());
            }
            return nodeL;
        } catch (RemoteException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * checkinNodes
     * Registers each Node into the Query system,
     * Returns boolean regarding staus of registering
     */
    private synchronized void checkInNodes(ArrayList<Node> nodeL) {
        if (nodeL.size() == 0) return;

        for (Node n : nodeL)
            try {
                registerNode(n);
            } catch (RemoteException e) {
                e.printStackTrace();
                return;
            }
    }

    /**
     * getHostIP
     * Determine the IP
     * Creates instance of server and adds/binds to registry
     */
    public void removeArray(Array a) throws RemoteException { //Remove Array from references
        arrayList.remove(a);
        try {
            Registry registry = LocateRegistry.getRegistry(Inet4Address.getLocalHost().getHostAddress(), QUERY.getQUERYPORT()); //IP Address of RMI Server, port of RMIRegistry
            InifQueryClient stub = (InifQueryClient) registry.lookup("QueryClient");
            stub.closeArray(a);
        } catch (Exception e) {
            System.out.println("Can't open Array to clients!");
        }

        System.err.println("Array Dissolved!");
    }

    public void stopQuery(String altQryIP, int altQryPrt) throws RemoteException {

    }

    public void stopQuery(String reason) throws RemoteException {
//        System.err.println("QueryServer Server Terminated! Reason: " + reason);
        ArrayList<Array> aList = arrayList;
        for (Array a : aList) {
            ArrayList<Node> nList = a.getNodeList();
            for (Node n : nList) {
                try {
                    Registry registry = LocateRegistry.getRegistry(n.getNodeIP(), n.getNodePort()); //IP Address of RMI Server, port of RMIRegistry
                    InifNode stub = (InifNode) registry.lookup("AdminNode");
                    try {
                        stub.terminateNode(reason);
                    } catch (Exception e) {
                        System.err.println("Node Terminated: IP:" + n.getNodeIP() + " Port:" + n.getNodePort());
                    }
                } catch (Exception e) {
                    System.out.println("\nCan't Contact Node!(Array)\n IP:" + n.getNodeIP() + " Port:" + n.getNodePort());
                }
            }
        }
        for (Node n : nodeList.values()) {
            try {
                Registry registry = LocateRegistry.getRegistry(n.getNodeIP(), n.getNodePort()); //IP Address of RMI Server, port of RMIRegistry
                InifNode stub = (InifNode) registry.lookup("AdminNode");
                try {
                    stub.terminateNode(reason);
                } catch (Exception e) {
                    System.err.println("Node Terminated: IP:" + n.getNodeIP() + " Port:" + n.getNodePort());
                }
            } catch (Exception e) {
                System.out.println("\nCan't Contact Node! (nodeList)\n IP:" + n.getNodeIP() + " Port:" + n.getNodePort());
            }
        }
        System.exit(1);
    }

    public void printUnassignedNodes() {
        try {
            System.out.println("Nodes Unassigned: " + nodeList.size());
            for (Node n : nodeList.values()) {
                System.out.println("Node IP: " + n.getNodeIP() + " Port: " + n.getNodePort());
            }
        } catch (Exception e) {
            System.out.println("Can't print Unassigned Nodes!");
        }
    }

    public ArrayList<Shard> getShardList() throws RemoteException {
        return SHARDS;
    }

    public ArrayList<Array> getArrayList() throws RemoteException {
        return null;
    }

    public ArrayList<Node> getUnassignedNodes() throws RemoteException {
        return null;
    }

    /**
     * ArrayCreate
     * Sets up new Array from unassigned Nodes
     * Executed on new Thread so
     */
    private class ArrayCreate implements Runnable {                                      //Concurrent thread that creates a new Array and dispatches it from Query Server
        Array arrayMeta = new Array();

        public void run() {
            ArrayList<Node> arNodeList = checkoutNodes(SHARDS.size());
            try {
                arrayMeta.setQueryIP(QUERY.getQUERYIP());
                arrayMeta.setQueryPort(QUERY.getQUERYPORT());
                for (Node n : arNodeList) { //One Node per Shard
                    n.setShard(SHARDS.get(arNodeList.indexOf(n)));
                    arrayMeta.addShardMap(n);
                    Registry registry = LocateRegistry.getRegistry(n.getNodeIP(), n.getNodePort()); //IP Address of RMI Server, port of RMIRegistry
                    registry.lookup("AdminServer"); //Verify Node is active
                    arrayMeta.addNode(n); //Add node to Array
                    for (Node o : arrayMeta.getNodeList()) {  //Transcribe arrayMeta to Nodes
                        startServices startNodes = new startServices(arrayMeta, o);
                        new Thread(startNodes).start();
                    }

                    arrayList.add(arrayMeta);
                    try {
                        registry = LocateRegistry.getRegistry(QUERY.getQUERYIP(), QUERY.getQUERYPORT()); //IP Address of RMI Server, port of RMIRegistry
                        InifQueryClient stub = (InifQueryClient) registry.lookup("QueryClient");
                        stub.openArray(arrayMeta);
                    } catch (Exception e) {
                        System.out.println("Can't open Array to clients!");
                    }
                }
            } catch (Exception e) {
                System.err.println("Unable to create new Array! (Ping)");
                arNodeList.remove(arNodeList.size() - 1);
                checkInNodes(arNodeList);
                System.out.println("Returned good Nodes to List!");

            }


        }

        class startServices implements Runnable { //Thread that starts each Node's services concurrently
            Array data;
            Node n;

            startServices(Array data, Node n) {
                this.data = data;
                this.n = n;
            }

            @Override
            public void run() {
                try {
                    Registry registry = LocateRegistry.getRegistry(n.getNodeIP(), n.getNodePort()); //IP Address of RMI Server, port of RMIRegistry
                    InifNode stub = (InifNode) registry.lookup("AdminNode"); //Name of RMI Server in registry
                    stub.setArrayData(data);
                    stub.setShard(n.getShard());
                    stub.startService();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}



