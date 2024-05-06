import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

public class CustomClient {
    private final static String INITIATE = "0";

    public static void main(String[] args) throws Exception {
        String customQueueName = "CUSTOM_QUEUE_" + args[0];
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(customQueueName, false, false, false, null);

            channel.basicPublish("", customQueueName, null, INITIATE.getBytes(StandardCharsets.UTF_8));
            System.out.printf(" [x] Sent INITIATE to %s\n", args[0]);
        }
    }
}
