package edu.ucsb.nceas;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
    // Options (configurable via command line args)
    private static String action = "poll";
    private static String address = "127.0.0.1:5701";
    private static String groupName = "";
    private static String groupPassword = "";
    private static int delay = 1000; // in milliseconds
    private static int duration = 60000; // in milliseconds

    private static IMap<Identifier, Object> indexQueue = null;
    private static HazelcastClient client = null;

    /**
     * @param args See implementation in parseArguments
     */
    public static void main(String[] args) {
        parseArguments(args);

        ClientConfig config = new ClientConfig();
        config.addAddress(address);

        GroupConfig groupConfig = new GroupConfig();
        groupConfig.setName(groupName);
        groupConfig.setPassword(groupPassword);

        config.setGroupConfig(groupConfig);

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

        if (action == "poll") {
            poll(indexQueue);
        } else if (action == "list") {
            listAll(indexQueue);
        }

        System.exit(0);
    }

    private static void poll(IMap<Identifier, Object> queue) {
        int count = duration / delay;

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
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void listAll(IMap<Identifier, Object> queue) {
        Iterator<Map.Entry<Identifier, Object>> it = queue.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<Identifier,Object> entry = it.next();
            Identifier identifier = (Identifier) entry.getKey();

            System.out.println(identifier.getValue().toString());
        }

        return;
    }

    private static void parseArguments(String[] args) {
        HashMap<String, String> options = new HashMap<String, String>();

        for (String arg : args) {
            String[] tokens = arg.split("[=]");

            if (tokens.length != 2) {
                System.out.println("Failed to parse argument `" + arg + "`.");
                continue;
            }

            options.put(tokens[0].trim(), tokens[1].trim());
        }

        // Handle action
        if (options.containsKey("action")) {
            action = options.get("action");
        }

        // Handle address
        if (options.containsKey("address")) {
            address = options.get("address");
        }

        // Handle groupName
        if (options.containsKey("groupName")) {
            groupName = options.get("groupName");
        }

        // Handle groupPassword
        if (options.containsKey("groupPassword")) {
            groupPassword = options.get("groupPassword");
        }

        // Handle delay
        if (options.containsKey("delay")) {
            try {
                delay = Integer.parseInt(options.get("delay"));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        // Handle duration
        if (options.containsKey("duration")) {
            try {
                duration = Integer.parseInt(options.get("duration"));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
