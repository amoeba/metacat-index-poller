package edu.ucsb.nceas;

import java.util.Map;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.IMap;

import org.dataone.service.types.v1.Identifier;

/**
 * Polls the index queue in a local Metacat, or
 * a remote Metacat via an SSH tunnel
 *
 * @author cjones
 *
 */
public class MetacatIndexPoller {
    static HashMap<String, String> options = new HashMap<String, String>();    
    private static IMap<Identifier, Object> indexQueue = null;
    private static ClientConfig config = new ClientConfig();
    private static HazelcastClient client = null;

    /**
     * @param args Options passed in as {KEY}={VALUE} pairs. Supported options
       include:
         - action (Default: "poll")
         - address (Default: "localhost:5701")
         - groupName (Default: "")
         - groupPassword (Default: "")
         - delay (Default: 1000 in milliseconds)
         - duration (Default: 60000 in milliseconds)

        Note: When action=evict, you must specify "pid={SOME_PID}"
     */
    public static void main(String[] args) {
        parseArguments(args);
        setupConfig();

        try {
            client = HazelcastClient.newHazelcastClient(config);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            indexQueue = client.getMap("hzIndexQueue");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }

        String action = options.get("action");

        if (action.equals("poll")) {
            poll();
        } else if (action.equals("list")) {
            list();
        } else if (action.equals("evict")) {
            String pid = options.get("pid");

            if (pid == null) {
                System.out.println("You must specifiy the \"pid\" argument to evict.");
                System.exit(1);
            }
            
            evictByPID(pid);
        } else {
            System.out.println("No action specified. Quitting.");
            System.exit(1);
        }

        System.exit(0);
    }

    private static void poll() {
        int count = 0; 

        try {
            count = Integer.parseInt(options.get("duration")) / Integer.parseInt(options.get("delay"));
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }

        // Poll the queue size up to `count` times
        for (int i = 0; i < count; i++) {
            try {
                indexQueue = client.getMap("hzIndexQueue");
                int size = indexQueue.size();

                System.out.println(size);
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }

            try {
                Thread.sleep(Integer.parseInt(options.get("delay")));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void list() {
        Iterator<Map.Entry<Identifier, Object>> it = indexQueue.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<Identifier,Object> entry = it.next();
            Identifier identifier = (Identifier) entry.getKey();

            System.out.println(identifier.getValue().toString());
        }

        return;
    }

    private static void evictByPID(String pid_string) {
        Identifier pid = new Identifier();
        pid.setValue(pid_string);

        if (!indexQueue.containsKey(pid)) {
            System.out.println("The index queue map doesn't contain an entry for " + pid_string + ". Quitting.");
            System.exit(1);
        }

        boolean result = false;

        try {
            result = indexQueue.evict(pid);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }

        if (!result) {
            System.out.println("Failed to evict the index task for the Object with PID " + pid_string + ".");
            System.exit(1);
        }
    }

    private static void parseArguments(String[] args) {
        // Load up defaults
        options.put("action", "poll");
        options.put("address", "localhost:5701");
        options.put("groupName", "");
        options.put("groupPassword", "");
        options.put("delay", "1000"); // milliseconds
        options.put("duration", "60000"); // milliseconds

        // Overwrite with any passed-in arguments
        for (String arg : args) {
            String[] tokens = arg.split("[=]");

            options.put(tokens[0].trim(), String.join("=", Arrays.copyOfRange(tokens, 1, tokens.length )));
        }

        for (String key : options.keySet()) {
            System.out.println(key + "=" + options.get(key));
        }
    }

    private static void setupConfig() {
        config.addAddress(options.get("address"));

        GroupConfig groupConfig = new GroupConfig();
        groupConfig.setName(options.get("groupName"));
        groupConfig.setPassword(options.get("groupPassword"));

        config.setGroupConfig(groupConfig);
    }
}
