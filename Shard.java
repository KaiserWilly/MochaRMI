
import java.io.Serializable;


/**
 * Created 10/24/2016
 * Software Development - Team 2063-1
 * Colorado TSA Conference - Feb 2017
 *
 * Purpose: Shard is an abstract class that gives
 * developers the ability to custom design shards
 * that fit their program. This means that this
 * module can run as an independent library.
 */
public abstract class Shard implements Serializable {
    private String role = "Unassigned";

    public Shard(String role) {
        this.role = role;
    }

    public String getRole() {
        return role;
    }

    public abstract void startShard(Array data, Node n);

}
