public class CustomNodeStart {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java CustomNodeStart <nodeId> <outputNodeId>");
            System.exit(1);
        }

        int nodeId = Integer.parseInt(args[0]);
        String outputNodeId = args[1];

        try {
            CustomNode customNode = new CustomNode();
            customNode.start(nodeId, outputNodeId);
        } catch (Exception e) {
            System.err.println("Error in CustomNode: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
