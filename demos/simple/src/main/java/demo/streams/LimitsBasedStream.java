package demo.streams;

import demo.support.Serde;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class LimitsBasedStream {

    public static void main(String[] args) throws Exception {

        Options options = Options.builder() //
                .connectionName("jugsaar") //
                .userInfo("jugsaar", "jugsaar") //
                .server("nats://localhost:4222") //
                .build();

        try (Connection conn = Nats.connect(options)) {

            JetStreamManagement jsm = conn.jetStreamManagement();

            jsm.deleteStream("EVENTS");

            StreamConfiguration sc = StreamConfiguration.builder() //
                    .name("EVENTS") //
                    .subjects("events.>") //
                    .storageType(StorageType.File) //
                    .build();

            StreamInfo si = jsm.addStream(sc);
            System.out.printf("""
                    Created stream %s
                    """, si.getConfiguration().getName());

            JetStream js = conn.jetStream();

            js.publish("events.page_loaded", null);
            js.publish("events.mouse_clicked", null);
            js.publish("events.mouse_clicked", null);
            js.publish("events.page_loaded", null);
            js.publish("events.mouse_clicked", null);
            js.publish("events.input_focused", null);
            System.out.println("Published 6 messages");

            js.publishAsync("events.input_changed", null);
            js.publishAsync("events.input_blurred", null);
            js.publishAsync("events.key_pressed", null);
            js.publishAsync("events.input_focused", null);
            js.publishAsync("events.input_changed", null);
            CompletableFuture<PublishAck> future = js.publishAsync("events.input_blurred", null);

            try {
                future.get(1, TimeUnit.SECONDS);
                System.out.println("Published 6 messages");
            } catch (TimeoutException toe) {
                System.out.println("Publishing took too long");
            }

            printStreamState(jsm, sc.getName());

            sc = StreamConfiguration.builder(sc) //
                    .maxMessages(10) //
                    .build();
            jsm.updateStream(sc);
            System.out.println("set max messages to 10");

            printStreamState(jsm, sc.getName());

            sc = StreamConfiguration.builder(sc) //
                    .maxBytes(300) //
                    .build();
            jsm.updateStream(sc);
            System.out.println("set max bytes to 300");

            printStreamState(jsm, sc.getName());

            sc = StreamConfiguration.builder(sc) //
                    .maxAge(Duration.ofSeconds(1)) //
                    .build();
            jsm.updateStream(sc);
            System.out.println("set max age to one second");

            printStreamState(jsm, sc.getName());

            System.out.println("sleeping one second...");
            TimeUnit.SECONDS.sleep(1);

            printStreamState(jsm, sc.getName());
        }
    }

    public static void printStreamState(JetStreamManagement jsm, String streamName) throws Exception {

        StreamInfo info = jsm.getStreamInfo(streamName);
        System.out.printf("""
                inspecting stream info
                %s
                """, Serde.json(info.getStreamState()));
    }
}
