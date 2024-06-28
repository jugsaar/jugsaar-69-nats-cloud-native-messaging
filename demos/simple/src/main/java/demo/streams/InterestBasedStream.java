package demo.streams;

import demo.support.Serde;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.PublishAck;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsJetStreamMetaData;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class InterestBasedStream {

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
                    .retentionPolicy(RetentionPolicy.Interest) //
                    .subjects("events.>") //
                    .build();

            StreamInfo si = jsm.addStream(sc);
            System.out.printf("Created stream %s%n", sc.getName());

            JetStream js = conn.jetStream();

            js.publish("events.page_loaded", null);
            js.publish("events.mouse_clicked", null);
            PublishAck publishAck = js.publish("events.input_focused", null);
            System.out.println("Published 3 messages");

            System.out.printf("last message seq: %d%n", publishAck.getSeqno());

            System.out.println("# Stream info without any consumers\n");
            printStreamState(jsm, sc.getName());

            ConsumerInfo c1 = jsm.createConsumer(sc.getName(), ConsumerConfiguration.builder() //
                    .durable("processor-1") //
                    .ackPolicy(AckPolicy.Explicit) //
                    .build());

            js.publish("events.mouse_clicked", null);
            js.publishAsync("events.input_focused", null);

            System.out.println("# Stream info with one consumer\n");
            printStreamState(jsm, sc.getName());

            PullSubscribeOptions o1 = PullSubscribeOptions.bind("EVENTS", c1.getName());
            JetStreamSubscription sub1 = js.subscribe(null, o1);

            List<Message> fetched = sub1.fetch(2, Duration.ofSeconds(1));
            for (var msg : fetched) {
                msg.ackSync(Duration.ofSeconds(1)); // wait for broker to confirm
            }

            System.out.println("# Stream info with one consumer and acked messages\n");
            printStreamState(jsm, sc.getName());

            ConsumerInfo c2 = jsm.createConsumer(sc.getName(), ConsumerConfiguration.builder() //
                    .durable("processor-2") //
                    .ackPolicy(AckPolicy.Explicit) //
                    .build());

            PullSubscribeOptions o2 = PullSubscribeOptions.bind("EVENTS", c2.getName());
            JetStreamSubscription sub2 = js.subscribe(null, o2);

            js.publish("events.input_focused", null);
            js.publish("events.mouse_clicked", null);

            fetched = sub2.fetch(2, Duration.ofSeconds(1));
            List<NatsJetStreamMetaData> messageMeta = new ArrayList<>();
            for (var msg : fetched) {
                msg.ackSync(Duration.ofSeconds(1));
                messageMeta.add(msg.metaData());
            }
            System.out.printf("msg seqs %d and %d%n", messageMeta.get(0).streamSequence(), messageMeta.get(1).streamSequence());

            System.out.println("# Stream info with two consumers, but only one set of acked messages\n");
            printStreamState(jsm, sc.getName());

            fetched = sub2.fetch(2, Duration.ofSeconds(1));
            for (var msg : fetched) {
                // double ack feature missing here!
//                msg.ackSync(Duration.ofSeconds(1));
                msg.ack();
            }

            System.out.println("# Stream info with two consumers having both acked\n");
            printStreamState(jsm, sc.getName());


            ConsumerInfo c3 = jsm.createConsumer(sc.getName(), ConsumerConfiguration.builder() //
                    .durable("processor-3") //
                    .ackPolicy(AckPolicy.Explicit) //
                    .filterSubject("events.mouse_clicked") // not interested in input_focussed
                    .build());

            PullSubscribeOptions o3 = PullSubscribeOptions.bind("EVENTS", c3.getName());
            JetStreamSubscription sub3 = js.subscribe(null, o3);

            js.publish("events.input_focused", null);

            var msgs = sub1.fetch(1, Duration.ofSeconds(1));
            var msg = msgs.get(0);
            msg.term();

            msgs = sub2.fetch(1, Duration.ofSeconds(1));
            msg = msgs.get(0);
            msg.ackSync(Duration.ofSeconds(1));

            System.out.println("# Stream info with three consumers with interest from two\n");
            printStreamState(jsm, sc.getName());
        }
    }

    public static void printStreamState(JetStreamManagement jsm, String streamName) throws Exception {

        StreamInfo info = jsm.getStreamInfo(streamName);
        System.out.printf("inspecting stream info%n%s%n", Serde.json(info.getStreamState()));
    }
}
