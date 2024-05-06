import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;

public class CustomNode {
    private String customQueueName = "";
    private int nodeId = 0;
    private String outputQueueName = "";
    private boolean initiated = false;

    private final static String INITIATE = "0";
    private final static String PING_MESSAGE = "1";
    private final static String PONG_MESSAGE = "2";

    public void start(int nodeId, String outputNodeId) throws Exception {
        
        // Initialization
        initialize(nodeId, outputNodeId);

        // RabbitMQ setup
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        declareQueues(channel);

        // Display a message indicating the node is waiting for messages
        System.out.printf(" [%d] Waiting! To exit, use CTRL+C\n", this.nodeId);

        // Callback function to handle incoming messages
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                handleMessage(channel, delivery);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        // Start consuming messages from the input queue with the defined callback
        channel.basicConsume(this.customQueueName, true, deliverCallback, consumerTag -> {});
    }

    // Additional function to initialize variables
    private void initialize(int nodeId, String outputNodeId) {
        this.nodeId = nodeId;
        this.outputQueueName = "CUSTOM_QUEUE_" + outputNodeId;
        this.customQueueName = "CUSTOM_QUEUE_" + this.nodeId;
    }

    // Additional function to declare input and output queues
    private void declareQueues(Channel channel) throws Exception {
        channel.queueDeclare(customQueueName, false, false, false, null);
        channel.queueDeclare(outputQueueName, false, false, false, null);
    }

    // Additional function to handle incoming messages
    private void handleMessage(Channel channel, Delivery delivery) throws Exception {
        String receivedMessage = new String(delivery.getBody(), StandardCharsets.UTF_8);

        if (receivedMessage.length() < 1) {
            System.err.printf(" [%d] Received wrong length message, ignore\n", this.nodeId);
            return;
        }

        String messageType = receivedMessage.substring(0, 1);

        switch (messageType) {
            case INITIATE:
                handleInitiate(channel);
                break;
            case PING_MESSAGE:
                handlePingMessage(channel, receivedMessage);
                break;
            case PONG_MESSAGE:
                handlePongMessage(channel, receivedMessage);
                break;
            default:
                System.err.printf(" [%s] Received wrong type message %s, ignore\n", this.nodeId, messageType);
        }
    }

    // Additional function to handle INITIATE messages
    private void handleInitiate(Channel channel) throws Exception {
        if (this.initiated) return;

        this.initiated = true;

        try {
            Thread.sleep(2000);
        } catch (Exception e) {
            System.err.printf(" [%d] Sleep error\n", this.nodeId);
        }

        String pingMessage = PING_MESSAGE + Integer.toString(this.nodeId);
        channel.basicPublish("", this.outputQueueName, null, pingMessage.getBytes("UTF-8"));
    }

    // Additional function to handle PING_MESSAGE
    private void handlePingMessage(Channel channel, String receivedMessage) throws Exception {
        int pingInputId = Integer.parseInt(receivedMessage.substring(1));

        System.out.printf(" [%d] Received a <PING> message from Node [%d]\n", this.nodeId, pingInputId);

        if (pingInputId >= this.nodeId) return;

        try {
            Thread.sleep(2000);
        } catch (Exception e) {
            System.err.printf(" [%d] Sleep error\n", this.nodeId);
        }

        String pongMessage = PONG_MESSAGE + Integer.toString(this.nodeId);
        channel.basicPublish("", this.outputQueueName, null, pongMessage.getBytes("UTF-8"));
    }

    // Additional function to handle PONG_MESSAGE
    private void handlePongMessage(Channel channel, String receivedMessage) throws Exception {
        int pongInputId = Integer.parseInt(receivedMessage.substring(1));

        System.out.printf(" [%d] Received a <PONG> message from Node [%d]\n", this.nodeId, pongInputId);

        if (pongInputId <= this.nodeId) return;

        try {
            Thread.sleep(2000);
        } catch (Exception e) {
            System.err.printf(" [%d] Sleep error\n", this.nodeId);
        }

        String pingMessage = PING_MESSAGE + Integer.toString(this.nodeId);
        channel.basicPublish("", this.outputQueueName, null, pingMessage.getBytes("UTF-8"));
    }
}
