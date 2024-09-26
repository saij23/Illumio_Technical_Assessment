
import java.io.*;
import java.util.*;

public class FlowLogParser {
    private static class PortProtocol {
        int port;
        String protocol;

        PortProtocol(int port, String protocol) {
            this.port = port;
            this.protocol = protocol;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PortProtocol that = (PortProtocol) o;
            return port == that.port && protocol.equals(that.protocol);
        }

        @Override
        public int hashCode() {
            return Objects.hash(port, protocol);
        }
    }

    private static Map<PortProtocol, List<String>> loadLookupTable(String filePath) throws IOException {
        Map<PortProtocol, List<String>> lookup = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                int dstport = Integer.parseInt(parts[0]);
                String protocol = parts[1].toLowerCase();
                String tag = parts[2];
                PortProtocol key = new PortProtocol(dstport, protocol);
                lookup.computeIfAbsent(key, k -> new ArrayList<>()).add(tag);
            }
        }
        return lookup;
    }

    private static void parseFlowLog(String logFile, Map<PortProtocol, List<String>> lookupTable,
                                     Map<String, Integer> tagCounts, Map<PortProtocol, Integer> portProtocolCounts) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(logFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.trim().split("\\s+");
                if (fields.length < 7) continue;

                int dstport = Integer.parseInt(fields[6]);
                String protocol = fields[5].equals("6") ? "tcp" : fields[5].equals("17") ? "udp" : "other";

                PortProtocol key = new PortProtocol(dstport, protocol);
                if (lookupTable.containsKey(key)) {
                    for (String tag : lookupTable.get(key)) {
                        tagCounts.merge(tag, 1, Integer::sum);
                    }
                } else {
                    tagCounts.merge("Untagged", 1, Integer::sum);
                }

                portProtocolCounts.merge(key, 1, Integer::sum);
            }
        }
    }

    private static void writeOutput(Map<String, Integer> tagCounts, Map<PortProtocol, Integer> portProtocolCounts, String outputFile) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
            writer.println("Tag Counts:");
            writer.println("Tag,Count");
            for (Map.Entry<String, Integer> entry : tagCounts.entrySet()) {
                writer.println(entry.getKey() + "," + entry.getValue());
            }

            writer.println("\nPort/Protocol Combination Counts:");
            writer.println("Port,Protocol,Count");
            for (Map.Entry<PortProtocol, Integer> entry : portProtocolCounts.entrySet()) {
                PortProtocol pp = entry.getKey();
                writer.println(pp.port + "," + pp.protocol + "," + entry.getValue());
            }
        }
    }

    public static void main(String[] args) {
        String logFile = "flow_log.txt";
        String lookupFile = "lookup_table.csv";
        String outputFile = "output.csv";

        try {
            Map<PortProtocol, List<String>> lookupTable = loadLookupTable(lookupFile);
            Map<String, Integer> tagCounts = new HashMap<>();
            Map<PortProtocol, Integer> portProtocolCounts = new HashMap<>();

            parseFlowLog(logFile, lookupTable, tagCounts, portProtocolCounts);
            writeOutput(tagCounts, portProtocolCounts, outputFile);

            System.out.println("Processing complete. Output written to " + outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
