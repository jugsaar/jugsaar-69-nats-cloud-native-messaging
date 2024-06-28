package demo.requestreply;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class SimpleRequestReply {

    public static void main(String[] args) {

        Options options = Options.builder() //
                .connectionName("jugsaar") //
                .userInfo("jugsaar", "jugsaar") //
                .server("nats://localhost:4222") //
                .build();

        try (Connection conn = Nats.connect(options)) {

            MessageHandler greeter = msg -> {
                String name = msg.getSubject().substring(6);
                String response = "hello " + name;
                conn.publish(msg.getReplyTo(), response.getBytes());
            };

            Dispatcher dispatcher = conn.createDispatcher(greeter);

            String greetSubject = "greet.*";

            dispatcher.subscribe(greetSubject);

            Message m = conn.request("greet.bob", null, Duration.ofSeconds(1));
            System.out.println("Response received: " + new String(m.getData()));


            try {
                CompletableFuture<Message> future = conn.request("greet.pam", null);
                m = future.get(1, TimeUnit.SECONDS);
                System.out.println("Response received: " + new String(m.getData()));
            } catch (ExecutionException e) {
                System.out.println("Something went wrong with the execution of the request: " + e);
            } catch (TimeoutException e) {
                System.out.println("We didn't get a response in time.");
            } catch (CancellationException e) {
                System.out.println("The request was cancelled due to no responders.");
            }

            dispatcher.unsubscribe(greetSubject);

            m = conn.request("greet.fred", null, Duration.ofMillis(300));
            System.out.println("Response was null? " + (m == null));


            try {
                CompletableFuture<Message> future = conn.request("greet.sue", null);
                m = future.get(1, TimeUnit.SECONDS);
                System.out.println("Response received: " + new String(m.getData()));
            } catch (ExecutionException e) {
                System.out.println("Something went wrong with the execution of the request: " + e);
            } catch (TimeoutException e) {
                System.out.println("We didn't get a response in time.");
            } catch (CancellationException e) {
                System.out.println("The request was cancelled due to no responders.");
            }

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}