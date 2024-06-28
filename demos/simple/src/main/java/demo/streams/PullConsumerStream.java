package demo.streams;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PullConsumerStream {

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
                    .build();

            StreamInfo si = jsm.addStream(sc);
            System.out.printf("""
                    Created stream %s
                    """, si.getConfiguration().getName());

            JetStream js = conn.jetStream();

            js.publish("events.1", null);
            js.publish("events.2", null);
            js.publish("events.3", null);

            final CountDownLatch[] waitGroup = {new CountDownLatch(3)};

            Dispatcher dispatcher = conn.createDispatcher();
            JetStreamSubscription sub1 = js.subscribe("events.>", //
                    dispatcher, //
                    msg -> {
                        System.out.println("Received msg on: " + msg.getSubject());
                        waitGroup[0].countDown();
                    },//
                    true //
            );
            waitGroup[0].await(10, TimeUnit.SECONDS);

            waitGroup[0] = new CountDownLatch(3);
            js.publish("events.4", null);
            js.publish("events.5", null);
            js.publish("events.6", null);


            List<Message> msgs = sub1.fetch(2, Duration.ofSeconds(1));
            int i = 0;
            for (var msg : msgs) {
                msg.ack();
                i++;
            }
            System.out.printf("Got %d messages%n", i);

            msgs = sub1.fetch(100, 0L);
            i = 0;
            for (var msg : msgs) {
                msg.ack();
                i++;
            }
            System.out.printf("Got %d messages%n", i);

            long fetchStart = System.currentTimeMillis();
            msgs = sub1.fetch(1, Duration.ofSeconds(1));
            i = 0;
            for (var msg : msgs) {
                msg.ack();
                i++;
            }
            System.out.printf("Got %d messages in %sms%n", i, Duration.ofMillis(System.currentTimeMillis() - fetchStart));

            ConsumerConfiguration c1 = ConsumerConfiguration.builder() //
                    .durable("processor") //
                    .build();

            jsm.addOrUpdateConsumer("EVENTS", c1);

            PullSubscribeOptions o1 = PullSubscribeOptions.bind("EVENTS", c1.getName());
            JetStreamSubscription dur = js.subscribe(null, o1);

            msgs = dur.fetch(1, 0);
            var msg = msgs.get(0);
            System.out.printf("Got %s from durable consumer%n", msg.getSubject());

            jsm.deleteConsumer(sc.getName(), c1.getName());

            ConsumerInfo consumerInfo = jsm.getConsumerInfo(sc.getName(), c1.getName());

        }
    }
}
