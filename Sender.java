import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

public class Sender {
    private final static String START = "0";
    private final static String QUEUE_PREFIX = "QUEUE";

    public static void main(String[] argv) throws Exception {
        if (argv.length != 3) {
            System.err.println("Error");
            return;
        }

        String queueName = QUEUE_PREFIX + Integer.toString(Integer.parseInt(argv[0]) - 1);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(queueName, false, false, false, null);

            String message = START + argv[1] + argv[2];
            channel.basicPublish("", queueName, null, message.getBytes(StandardCharsets.UTF_8));
            System.out.printf(" [x] Sent START to %s\n", argv[0]);
        }
    }
}
