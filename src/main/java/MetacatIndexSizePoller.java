import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.IMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Polls the index queue in a local Metacat, or
 * a remote Metacat via an SSH tunnel
 *
 * @author cjones
 *
 */
public class MetacatIndexSizePoller {
    // Options (configurable via command line args)
    private static String address = "127.0.0.1:5701";
    private static String groupName = "";
    private static String groupPassword = "";
    private static int delay = 5000; // in milliseconds
    private static int duration = 60000; // in milliseconds

    private static IMap<Object, Object> indexQueue = null;
    private static HazelcastClient hzClient = null;

    /**
     * @param args
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
            hzClient = HazelcastClient.newHazelcastClient(config);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }

        int count = duration / delay;

        // Poll the queue size up to `count` times
        for (int i = 0; i < count; i++) {
            try {
                indexQueue = hzClient.getMap("hzIndexQueue");
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

        System.exit(0);
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