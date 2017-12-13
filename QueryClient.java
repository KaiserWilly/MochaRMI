
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created 12/7/2016
 * Software Development - Team 2063-1
 * Colorado TSA Conference - Feb 2017
 *
 * Purpose: QueryClient is the implementation
 * of the server responsible for handling
 * incoming Client connections. It controls if
 * Arrays can accept clients, and directs clients
 * to valid Arrays
 */Array(Client c) throws RemoteException {
        List<Integer> counts = new ArrayList<>(); //Count of current clients by Array
        for (Array a : arrayList) {
            counts.add(balance.get(a).size());
        }
        int min = Integer.MAX_VALUE;
        for (int i : counts) {
            public class QueryClient implements InifQueryClient {
                private List<Array> arrayList = new ArrayList<>();
                private HashMap<Array, List<Client>> balance = new HashMap<>(); //Load-balancing Arrays

                public Array assignTo
            if (min > i) min = i;
        }
        Array server = arrayList.get(counts.indexOf(min));
        List<Client> playerList = balance.get(server);
        playerList.add(c);
        balance.replace(server, playerList);
        System.out.println("Client Connected (" + c.getClientIP() + ")! Directed to (" + server.getShardMap().get("Core").getNodeIP() + ")");
        return server;
    }

    public void openArray(Array data) { //Open Array to Clients
        arrayList.add(data);
        balance.put(data, new ArrayList<>());
        try {
            System.out.println("Array Opened to Clients! (" + data.getShardMap().get("Core").getNodeIP() + ")");
        } catch (RemoteException e) {
            System.out.println("Array Opened to Clients!");
        }
    }

    public void closeArray(Array data) {//Close Array from client connections
        arrayList.remove(data);
        System.out.println("Array Closed to Clients!");
    }
}
