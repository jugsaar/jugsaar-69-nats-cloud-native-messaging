package demo.pubsub;

import demo.support.Serde;
import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SimpleMessageSender {

    public static void main(String[] args) throws Exception {

        Options options = Options.builder() //
                .connectionName("jugsaar") //
                .userInfo("jugsaar", "jugsaar") //
                .server("nats://localhost:4222") //
                .connectionListener((conn, type) -> { //
                    System.out.printf("Handle connectionEvent %s %s %n", type, conn);
                }) //
                .errorListener(new ErrorListener() { //
                    @Override
                    public void exceptionOccurred(Connection conn, Exception exp) {
                        System.err.printf("exceptionOccurred %s %s %n", exp.getMessage(), conn);
                    }
                }) //
                .build();

        try (Connection conn = Nats.connect(options)) {
            System.out.println("Connected to Nats server");
            for (int i = 0; i < 100; i++) {
                var data = Map.of("id", UUID.randomUUID(), "timestamp", System.currentTimeMillis(), "data", i);
                byte[] messageBytes = Serde.jsonBytes(data);
                conn.publish("jugsaar.demo.topic", messageBytes);
                System.out.println("Sent message: " + data);

                TimeUnit.MILLISECONDS.sleep(500);
            }
        }
    }

}
