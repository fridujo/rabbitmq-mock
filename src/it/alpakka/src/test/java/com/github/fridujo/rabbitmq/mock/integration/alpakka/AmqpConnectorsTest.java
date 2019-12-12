package com.github.fridujo.rabbitmq.mock.integration.alpakka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.github.fridujo.rabbitmq.mock.compatibility.MockConnectionFactoryFactory;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.UniqueKillSwitch;
import akka.stream.alpakka.amqp.AmqpConnectionFactoryConnectionProvider;
import akka.stream.alpakka.amqp.AmqpConnectionProvider;
import akka.stream.alpakka.amqp.AmqpReplyToSinkSettings;
import akka.stream.alpakka.amqp.AmqpWriteSettings;
import akka.stream.alpakka.amqp.NamedQueueSourceSettings;
import akka.stream.alpakka.amqp.QueueDeclaration;
import akka.stream.alpakka.amqp.ReadResult;
import akka.stream.alpakka.amqp.WriteMessage;
import akka.stream.alpakka.amqp.javadsl.AmqpRpcFlow;
import akka.stream.alpakka.amqp.javadsl.AmqpSink;
import akka.stream.alpakka.amqp.javadsl.AmqpSource;
import akka.stream.alpakka.amqp.javadsl.CommittableReadResult;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import scala.collection.JavaConverters;
import scala.concurrent.duration.Duration;

/**
 * Shamefully copied from the <a href="https://github.com/akka/alpakka">Alpakka project</a> (under Apache 2.0 license).
 */
class AmqpConnectorsTest {

    private static ActorSystem system;
    private static Materializer materializer;

    private AmqpConnectionProvider connectionProvider = AmqpConnectionFactoryConnectionProvider.create(MockConnectionFactoryFactory.build());

    @BeforeAll
    public static void setUp() {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
    }

    @AfterAll
    public static void tearDown() {
        TestKit.shutdownActorSystem(system);
    }

    @AfterEach
    public void checkForStageLeaks() {
        StreamTestKit.assertAllStagesStopped(materializer);
    }

    @Test
    public void publishAndConsumeRpcWithoutAutoAck() throws Exception {

        final String queueName = "amqp-conn-it-spec-rpc-queue-" + System.currentTimeMillis();
        final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);

        final List<String> input = Arrays.asList("one", "two", "three", "four", "five");

        final Flow<WriteMessage, CommittableReadResult, CompletionStage<String>> ampqRpcFlow =
            AmqpRpcFlow.committableFlow(
                AmqpWriteSettings.create(connectionProvider)
                    .withRoutingKey(queueName)
                    .withDeclaration(queueDeclaration),
                10,
                1);
        Pair<CompletionStage<String>, TestSubscriber.Probe<ReadResult>> result =
            Source.from(input)
                .map(ByteString::fromString)
                .map(bytes -> WriteMessage.create(bytes))
                .viaMat(ampqRpcFlow, Keep.right())
                .mapAsync(1, cm -> cm.ack().thenApply(unused -> cm.message()))
                .toMat(TestSink.probe(system), Keep.both())
                .run(materializer);

        result.first().toCompletableFuture().get(5, TimeUnit.SECONDS);

        Sink<WriteMessage, CompletionStage<Done>> amqpSink =
            AmqpSink.createReplyTo(AmqpReplyToSinkSettings.create(connectionProvider));

        final Source<ReadResult, NotUsed> amqpSource =
            AmqpSource.atMostOnceSource(
                NamedQueueSourceSettings.create(connectionProvider, queueName)
                    .withDeclaration(queueDeclaration),
                1);

        UniqueKillSwitch sourceToSink =
            amqpSource
                .viaMat(KillSwitches.single(), Keep.right())
                .map(b -> WriteMessage.create(b.bytes()).withProperties(b.properties()))
                .to(amqpSink)
                .run(materializer);

        List<ReadResult> probeResult =
            JavaConverters.seqAsJavaListConverter(
                result.second().toStrict(Duration.create(5, TimeUnit.SECONDS)))
                .asJava();
        assertEquals(
            probeResult.stream().map(s -> s.bytes().utf8String()).collect(Collectors.toList()), input);
        sourceToSink.shutdown();
    }

    @Test
    public void keepConnectionOpenIfDownstreamClosesAndThereArePendingAcks() throws Exception {
        final String queueName = "amqp-conn-it-spec-simple-queue-" + System.currentTimeMillis();
        final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);

        final Sink<ByteString, CompletionStage<Done>> amqpSink =
            AmqpSink.createSimple(
                AmqpWriteSettings.create(connectionProvider)
                    .withRoutingKey(queueName)
                    .withDeclaration(queueDeclaration));

        final Integer bufferSize = 10;
        final Source<CommittableReadResult, NotUsed> amqpSource =
            AmqpSource.committableSource(
                NamedQueueSourceSettings.create(connectionProvider, queueName)
                    .withDeclaration(queueDeclaration),
                bufferSize);

        final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
        Source.from(input)
            .map(ByteString::fromString)
            .runWith(amqpSink, materializer)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);

        final CompletionStage<List<CommittableReadResult>> result =
            amqpSource.take(input.size()).runWith(Sink.seq(), materializer);

        List<CommittableReadResult> committableMessages =
            result.toCompletableFuture().get(3, TimeUnit.SECONDS);

        assertEquals(input.size(), committableMessages.size());
        committableMessages.forEach(
            cm -> {
                try {
                    cm.ack(false).toCompletableFuture().get(3, TimeUnit.SECONDS);
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            });
    }
}
