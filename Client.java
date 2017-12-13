
import java.io.Serializable;

/**
 * Created 12/7/2016
 * Software Development - Team 2063-1
 * Colorado TSA Conference - Feb 2017
 *
 * Purpose: Client defines a user attempting to connect to the server.
 * This class contains both the user's networking arrayMeta as well as the
 * networking arrayMeta provided by the user to the query server in a easy
 * Object, for clean archiving by the server and client.
 * NOTE: OSI Model is a standard networking model used to
 * describe communications from one user to another.
 */

public class Client implements Serializable {
    private Array server; // Array assigned by server
    private String clientIP, coreIP; //OSI Layer 3 Addresses for source and array
    private int clientPort, corePort;//Port configurations for source and array

    public Client(String clientIP, int clientPort) {
        this.clientIP = clientIP;
        this.clientPort = clientPort;
    }

    public void setServer(Array server) { // Sets server in Client, and gathers Core arrayMeta from array.
        this.server = server;
        try {
            setCore(server.getShardMap().get("Core").getNodeIP(), server.getShardMap().get("Core").getNodePort());
            //Get core arrayMeta in order to contact and register with Array
        } catch (Exception e) { //Data not set or not retrievable
            System.out.println("Unable to set Core in Client");
        }
    }

    public void setCore(String ip, int port) { // Set core arrayMeta for client
        coreIP = ip;
        corePort = port;
    }

    public String getCoreIP() {
        return coreIP;
    } //Get core IP (OSI Layer 3)

    public int getCorePort() {
        return corePort;
    } //Get Core Port

    public String getClientIP() {
        return clientIP;
    } //get Client IP (OSI Layer 3)

    public int getClientPort() {
        return clientPort;
    }//Get Core Port

    public Array getServer(){
        return server;
    }
}
