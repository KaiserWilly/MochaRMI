import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

/**
 * MochaRMI - Decentralized Java RMI Framework
 * (c) JD Isenhart
 * Updated September 2017
 * <p>
 * Query is the launching point for the framework.
 * Users create an instance of Query, and feed it
 * a list of Shards, which become the individual
 * nodes in a decentralized lot.
 */
public class Query {
    private ArrayList<Shard> shardList = null; //Class-wide Shard registry
    private final String QUERYNAME;
    private final int QUERYPORT;
    private final String QUERYIP;

    /**
     * Constructor - Query Service
     * Takes in all applicable Shards from user
     * Adds required Core shard + Readies Query
     */
    public Query(ArrayList<Shard> shardList, String queryName, int queryPort) {
        QUERYPORT = queryPort;
        QUERYNAME = queryName;
        QUERYIP = getHostIP();
        shardList.add(new CoreShard());     //Add required Core Shard to List
        this.shardList = shardList;         //Add all supplied Shards to Query's pool
    }

    /**
     * Constructor - Query Object
     * Acts as a container for all relevant query information
     * Passed onto Shards for reference
     */
    public Query(String queryName, ArrayList<Shard> shards, String queryIP, int queryPort) {
        QUERYPORT = queryPort;
        QUERYNAME = queryName;
        QUERYIP = getHostIP();
        shards.add(new CoreShard());     //Add required Core Shard to List
        this.shardList = shards;         //Add all supplied Shards to Query's pool
    }

    /**
     * startQuery - int Port
     * Initializes local RMI server, reserving port given as parameter
     * Loads Lot Server and Client Server
     * Starts IOConsole for Query Object
     */
    public boolean startQuery() {
        try {
            LocateRegistry.createRegistry(QUERYPORT);
        } catch (Exception e) {             //Catch if unable to create registry
            System.err.println("Unable to create Query Registry");
            return false;
        }
        startQueryServer();
        startQueryClient();
        System.out.println("Query Server Created!");
        System.out.println("IP Address: " + QUERYIP);
        System.out.println("Port: " + QUERYPORT);
        new QueryIOConsole(QUERYIP, QUERYPORT).run();    //Start Admin Query IO Console
        return true;
    }

    /**
     * StartQueryServer - int Port
     * Initializes Query side Node communication
     * Creates instance of server and adds/binds to registry
     */
    private void startQueryServer() {
        try {
            QueryServer obj = new QueryServer(getQueryMeta());                                  //Create new instance of content for RMI to use
            Registry registry = LocateRegistry.getRegistry(QUERYPORT);                          //Denote port to get registry from; create Registry
            registry.bind("QueryServer", UnicastRemoteObject.exportObject(obj, 0)); //Bind stub to registry

            System.out.println("Query Server \"QueryServer\" Started!");
        } catch (Exception e) {
            System.err.println("Can't create Query: Server");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * StartQueryClient - int Port
     * Initializes Query side Client communication
     * Creates instance of server and adds/binds to registry
     */
    private void startQueryClient() {
        try {
            QueryClient obj = new QueryClient();                                                // Create new instance of content for RMI to use
            Registry registry = LocateRegistry.getRegistry(QUERYPORT);                          //Denote port to get registry from; create Registry
            registry.bind("QueryClient", UnicastRemoteObject.exportObject(obj, 0)); //Bind stub to registry

            System.out.println("Query Server \"QueryClient\" Started!");
        } catch (Exception e) {
            System.err.println("Can't create Query: Client");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * getHostIP
     * Determine the IP address of the host platform
     * OS Dependent method of retrieval
     */
    static String getHostIP() {
        try {
            String os = System.getProperty("os.name").toLowerCase().substring(0, 3);             //Capture OS name, first 3 letters

            switch (os) {
                case "win":                                                                     //Denotes Windows OS
                    return Inet4Address.getLocalHost().getHostAddress();
                case "mac":                                                                     //Denotes Mac OS
                    List<String> privateIP = Arrays.asList("10", "172", "192");
                    String ip;
                    Enumeration<NetworkInterface> ni = NetworkInterface.getNetworkInterfaces(); //Get all available network interfaces
                    while (ni.hasMoreElements()) {                                              //Parse through each
                        Enumeration<InetAddress> ia = ni.nextElement().getInetAddresses();
                        while (ia.hasMoreElements()) {
                            ip = ia.nextElement().getHostAddress();
                            String[] ipA = ip.split(".");
                            if (privateIP.contains(ipA[0])) return ip;
                        }
                    }
                    return "0.0.0.0";
                case "lin":                                                                     //Denotes Linux OS
                case "fre":                                                                     //Denotes Solaris OS
                case "sun":                                                                     //Denotes Sun OS
                default:
                    try (final DatagramSocket socket = new DatagramSocket()) {
                        socket.connect(InetAddress.getByName("8.8.8.8"), 10002);           //Get prime interface
                        return socket.getLocalAddress().getHostAddress();
                    }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "0.0.0.0";
        }
    }

    private Query getQueryMeta() {
        return new Query(QUERYNAME, shardList, QUERYIP, QUERYPORT);                         //Wrap Query arrayMeta into container
    }

    ArrayList<Shard> getShardList() {
        return shardList;
    }

    int getQUERYPORT() {
        return QUERYPORT;
    }

    String getQUERYIP() {
        return QUERYIP;
    }
}
