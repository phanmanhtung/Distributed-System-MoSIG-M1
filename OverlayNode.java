import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class OverlayNode {
    
    private int totalNodeNum;          // Total number of nodes in the network
    private int curNodeId;             // ID of the current node
    private boolean[][] globalPhysicalTopology; 
    private int[] nextHop;             // Next-hop routing decisions
    private int virtualRingPrev;       // ID of the previous node in the virtual ring
    private int virtualRingNext;       // ID of the next node in the virtual ring

    private void generateNextHop() {
        // Initialize arrays to store previous nodes and distances
        int[] prev = new int[totalNodeNum], distance = new int[totalNodeNum];
        Arrays.fill(prev, -1);
        Arrays.fill(distance, Integer.MAX_VALUE);
        
        // Breadth-first search to compute shortest paths
        Queue<Integer> queue = new LinkedList<>();
        queue.offer(curNodeId);
        distance[curNodeId] = 0;
        while (!queue.isEmpty()) {
            int currentNode = queue.poll();
            for (int neighbor = 0; neighbor < totalNodeNum; neighbor++) {
                // Check if there is a link to the neighbor and update distance if shorter
                if (globalPhysicalTopology[currentNode][neighbor] && distance[currentNode] + 1 < distance[neighbor]) {
                    distance[neighbor] = distance[currentNode] + 1;
                    prev[neighbor] = currentNode;
                    queue.offer(neighbor);
                }
            }
        }
        
        // Populate the next-hop routing table based on shortest paths
        nextHop = new int[totalNodeNum];
        for (int i = 0; i < totalNodeNum; i++) {
            int j = i;
            // Trace back to find the next-hop node for each destination
            while (prev[j] != -1 && prev[j] != curNodeId) {
                j = prev[j];
            }
            nextHop[i] = j;
        }
    }

    // Constants for message types and queue prefix
    private static final String QUEUE_PREFIX = "QUEUE";
    private static final String MESSAGE_TYPE_START = "0";
    private static final String MESSAGE_TYPE_SEND = "1";

    // Method to start the node's operation
    public void start(int nodeId) throws Exception {
        // Initialize node properties
        curNodeId = nodeId - 1;
        int[][] physicalTopology = {
                {0, 0, 1, 0, 0},
                {0, 0, 1, 1, 1},
                {1, 1, 0, 0, 0},
                {0, 1, 0, 0, 1},
                {0, 1, 0, 1, 0}
        };
        int[] virtualTopology = {1, 4, 3, 5, 2};
        totalNodeNum = physicalTopology.length;
        globalPhysicalTopology = new boolean[totalNodeNum][totalNodeNum];
        
        // Convert physical topology matrix to adjacency matrix
        for (int i = 0; i < totalNodeNum; i++) {
            for (int j = 0; j < totalNodeNum; j++) {
                globalPhysicalTopology[i][j] = physicalTopology[i][j] == 1;
            }
        }
        
        // Determine previous and next nodes in the virtual ring
        for (int i = 0; i < totalNodeNum; i++) {
            if (virtualTopology[i] == nodeId) {
                virtualRingPrev = virtualTopology[(i + totalNodeNum - 1) % totalNodeNum] - 1;
                virtualRingNext = virtualTopology[(i + 1) % totalNodeNum] - 1;
            }
        }
        
        // Generate next-hop routing table
        generateNextHop();
        
        // Setup RabbitMQ connection and channels
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        
        // Declare queues for message passing
        String queueName = QUEUE_PREFIX + curNodeId;
        channel.queueDeclare(queueName, false, false, false, null);
        for (int i = 0; i < totalNodeNum; i++) {
            if (i != curNodeId && globalPhysicalTopology[curNodeId][i]) {
                channel.queueDeclare(QUEUE_PREFIX + i, false, false, false, null);
            }
        }
        
        // Start consuming messages from the node's queue
        System.out.printf("[%d] Waiting for messages.\n", curNodeId + 1);
        channel.basicConsume(queueName, true, (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            if (message.length() < 1) {
                // Ignore invalid messages
                System.err.printf("[%d] Received invalid message, ignoring.\n", curNodeId + 1);
                return;
            }
            String messageType = message.substring(0, 1);
            if (messageType.equals(MESSAGE_TYPE_START)) {
                // Handle START message
                handleStartMessage(message, channel);
            } else if (messageType.equals(MESSAGE_TYPE_SEND)) {
                // Handle SEND message
                handleSendMessage(message, channel);
            } else {
                // Unknown message type
                System.err.printf("[%d] Received unknown message type: %s\n", curNodeId + 1, messageType);
            }
        }, consumerTag -> {});
    }

    // Method to handle START message
    private void handleStartMessage(String message, Channel channel) throws IOException {
        // Parse START message and determine target node
        System.out.printf("[%d] Received <START> message.\n", curNodeId + 1);
        String direction = message.substring(1, 2);
        int target = direction.equals("L") ? virtualRingPrev : virtualRingNext;
        // Send SEND message to the next-hop node in the virtual ring
        String nextQueue = QUEUE_PREFIX + nextHop[target];
        String sendMessage = String.format("%s%d %d %s", MESSAGE_TYPE_SEND, target, curNodeId, message.substring(2));
        channel.basicPublish("", nextQueue, null, sendMessage.getBytes(StandardCharsets.UTF_8));
    }

    // Method to handle SEND message
    private void handleSendMessage(String message, Channel channel) throws IOException {
        // Parse SEND message
        String[] parts = message.substring(1).split(" ", 3);
        int target = Integer.parseInt(parts[0]);
        int originalSender = Integer.parseInt(parts[1]);
        String messageContent = parts[2];
        
        if (target == curNodeId) {
            // If the current node is the target, print the message and indicate reaching destination
            System.out.printf("Node [%d] Received message: %s\n", curNodeId + 1, messageContent);
            System.out.printf("Reaching destination.\n");
            return;
        }
        
        // Determine the next node in the physical ring and send the message
        int nextPhysicalNode = nextHop[target];
        String nextPhysicalQueue = QUEUE_PREFIX + nextPhysicalNode;
        // Print message passing information for intermediate nodes
        System.out.printf("Node [%d] Received message: %s\n", curNodeId + 1, messageContent);
        System.out.printf("Original message from Node [%d], received from previous Node [%d], pass to next Node [%d]\n", originalSender + 1, curNodeId + 1, nextPhysicalNode + 1);
        channel.basicPublish("", nextPhysicalQueue, null, message.getBytes(StandardCharsets.UTF_8));
    }
}
