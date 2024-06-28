package demo.pubsub;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.Subscription;

import java.util.concurrent.TimeUnit;

public class SimpleQueue {

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

            String workSubject = "work";
            String queueName = "myQueue";

            conn.publish(workSubject, ("data " + System.currentTimeMillis()).getBytes());

            Dispatcher dispatcher = conn.createDispatcher();
            Subscription subWorker1 = dispatcher.subscribe(workSubject, queueName, msg -> {
                System.out.println("Worker1 processes: " + new String(msg.getData()));
            });
            Subscription subWorker2 = dispatcher.subscribe(workSubject, queueName, msg -> {
                System.out.println("Worker2 processes: " + new String(msg.getData()));
            });

            System.out.println("2 worker subscribed");

            System.out.println("Sending messages...");
            for (int i = 0; i < 5; i++) {
                conn.publish(workSubject, ("data " + i + " " + System.currentTimeMillis()).getBytes());
            }

            TimeUnit.SECONDS.sleep(2);

            dispatcher.unsubscribe(subWorker1);

            System.out.println("1 worker subscribed");

            System.out.println("Sending messages...");
            for (int i = 0; i < 5; i++) {
                conn.publish(workSubject, ("data " + i + " " + System.currentTimeMillis()).getBytes());
            }

            System.out.println("0 worker subscribed");

            TimeUnit.SECONDS.sleep(2);

            dispatcher.unsubscribe(subWorker2);

            System.out.println("Sending messages...");
            for (int i = 0; i < 5; i++) {
                conn.publish(workSubject, ("data " + i + " " + System.currentTimeMillis()).getBytes());
            }
        }
    }
}
