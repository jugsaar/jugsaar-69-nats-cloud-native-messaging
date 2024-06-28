package demo.pubsub;

import demo.support.Serde;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SimplePubSub {

    public static void main(String[] args) throws Exception {

        Options options = Options.builder() //
                .connectionName("jugsaar") //
                .userInfo("jugsaar", "jugsaar") //
                .server("nats://localhost:4222") //
                .executor(Executors.newVirtualThreadPerTaskExecutor()) //
                .build();

        try (Connection conn = Nats.connect(options)) {
            System.out.println("Connected to Nats server");

            Dispatcher dispatcher = conn.createDispatcher();
            String subject =  //
//                    "jugsaar.demo.topic" // exact
                    // "jugsaar.demo.*" // wildcard
                    "jugsaar.>" // subtree
                    ;
            dispatcher.subscribe(subject, msg -> {
                try {
                    var payload = Serde.fromJsonBytes(msg.getData(), Map.class);
                    System.out.printf("""
                            Received message payload: %s on subject: %s in thread %s
                            """, payload, msg.getSubject(), Thread.currentThread());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

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
