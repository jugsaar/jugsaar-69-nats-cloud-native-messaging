package demo.streams;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.time.Duration;

public class WorkQueueStream {

    public static void main(String[] args) {

        Options options = Options.builder() //
                .connectionName("jugsaar") //
                .userInfo("jugsaar", "jugsaar") //
                .server("nats://localhost:4222") //
                .build();


        try (Connection conn = Nats.connect(options)) {

            JetStreamManagement jsm = conn.jetStreamManagement();

            StreamConfiguration sc = StreamConfiguration.builder() //
                    .name("EVENTS") //
                    .subjects("events.>") //
                    .retentionPolicy(RetentionPolicy.WorkQueue) //
                    .build();

            StreamInfo si = jsm.addStream(sc);
            System.out.printf("""
                    Created stream %s
                    """, si.getConfiguration().getName());

            JetStream js = conn.jetStream();

            js.publish("events.us.page_loaded", "time_ms: 120".getBytes());
            js.publish("events.eu.mouse_clicked", "x: 510, y: 1039".getBytes());
            js.publish("events.us.input_focused", "id: username".getBytes());
            System.out.println("Published 3 messages");

            StreamInfo info = jsm.getStreamInfo("EVENTS");
            long msgCount = info.getStreamState().getMsgCount();
            System.out.printf("msgs in stream: %s%n%n", msgCount);

            ConsumerConfiguration c1 = ConsumerConfiguration.builder() //
                    .durable("processor-1") //
                    .ackPolicy(AckPolicy.Explicit) //
                    .build();

            jsm.addOrUpdateConsumer("EVENTS", c1);

            PullSubscribeOptions o1 = PullSubscribeOptions.bind("EVENTS", "processor-1");
            JetStreamSubscription sub1 = js.subscribe(null, o1);

            sub1.pull(5);
            for (int i = 0; i < 5; i++) {
                Message msg = sub1.nextMessage(Duration.ofSeconds(1));
                if (msg == null) {
                    break;
                }
                msg.ack();
                System.out.printf("Received event: %s\n", msg.getSubject());
            }
            sub1.unsubscribe();


            info = jsm.getStreamInfo("EVENTS");
            msgCount = info.getStreamState().getMsgCount();
            System.out.printf("""
                    msgs in stream: %s
                    """, msgCount);


            ConsumerConfiguration c2 = ConsumerConfiguration.builder().durable("processor-2") //
                    .ackPolicy(AckPolicy.Explicit) //
                    .build();


            try {
                jsm.addOrUpdateConsumer("EVENTS", c2);
            } catch (JetStreamApiException e) {
                System.out.printf("""
                        Failed to create an overlapping consumer:
                        %s
                        """, e.getMessage());
            }


            jsm.deleteConsumer("EVENTS", "processor-1");
            System.out.println("Deleting the first consumer");


            jsm.addOrUpdateConsumer("EVENTS", c2);
            System.out.println("Succeeded to create a new consumer");
            jsm.deleteConsumer("EVENTS", "processor-2");
            System.out.println("Deleting the new consumer\n");


            ConsumerConfiguration c3 = ConsumerConfiguration.builder().durable("processor-3") //
                    .ackPolicy(AckPolicy.Explicit) //
                    .filterSubject("events.us.>") //
                    .build();


            jsm.addOrUpdateConsumer("EVENTS", c3);


            PullSubscribeOptions o3 = PullSubscribeOptions.bind("EVENTS", "processor-3");


            JetStreamSubscription sub3 = js.subscribe(null, o3);


            ConsumerConfiguration c4 = ConsumerConfiguration.builder() //
                    .durable("processor-4") //
                    .ackPolicy(AckPolicy.Explicit) //
                    .filterSubject("events.eu.>") //
                    .build();


            jsm.addOrUpdateConsumer("EVENTS", c4);


            PullSubscribeOptions o4 = PullSubscribeOptions.bind("EVENTS", "processor-4");


            JetStreamSubscription s4 = js.subscribe(null, o4);
            System.out.println("Created two filtered consumers");


            js.publish("events.us.page_loaded", "time_ms: 103".getBytes());
            js.publish("events.eu.mouse_clicked", "x: 1040, y: 39".getBytes());
            js.publish("events.us.input_focused", "id: password".getBytes());
            System.out.println("Published 3 more messages");


            sub3.pull(2);
            for (int i = 0; i < 2; i++) {
                Message msg = sub3.nextMessage(Duration.ofSeconds(1));
                msg.ack();
                System.out.printf("Received event via 'processor-us': %s\n", msg.getSubject());
            }


            s4.pull(1);
            Message msg = s4.nextMessage(Duration.ofSeconds(1));
            msg.ack();
            System.out.printf("Received event via 'processor-eu': %s\n", msg.getSubject());


            sub3.unsubscribe();
            jsm.deleteConsumer("EVENTS", "processor-3");


            s4.unsubscribe();
            jsm.deleteConsumer("EVENTS", "processor-4");


            jsm.deleteStream("EVENTS");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
