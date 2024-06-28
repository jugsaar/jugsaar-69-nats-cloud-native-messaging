package demo.pubsub;

import demo.support.Serde;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.util.Map;

public class SimpleMessageReceiver {

    public static void main(String[] args) throws Exception {

        Options options = Options.builder() //
                .connectionName("jugsaar") //
                .userInfo("jugsaar", "jugsaar") //
                .server("nats://localhost:4222") //
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
                    System.out.printf("""
                            Received message: %s on subject: %s
                            """, Serde.fromJsonBytes(msg.getData(), Map.class), msg.getSubject());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            Thread.sleep(60000);
        }
    }

}
