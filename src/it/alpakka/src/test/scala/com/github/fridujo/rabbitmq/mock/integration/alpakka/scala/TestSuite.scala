import java.util

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.amqp.{AmqpCachedConnectionProvider, AmqpConnectionFactoryConnectionProvider}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.github.fridujo.rabbitmq.mock.{MockChannel, MockConnectionFactory}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{AMQP, Channel, Connection, ConnectionFactory, DefaultConsumer, Envelope}
import org.scalatest.funsuite.AnyFunSuiteLike

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
  * @author mdiasribeiro
  */
class TestSuite extends TestKit(ActorSystem("TestSuite")) with ImplicitSender with AnyFunSuiteLike {

    def bindActorToQueue(channel: Channel, receiver: ActorRef, queueName: String, consumerTag: String = "myConsumerTag"): Unit = {
        channel.basicConsume(queueName, true, consumerTag, new DefaultConsumer(channel) {
            override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
                receiver ! new String(body)
            }
        })
    }

    class ProducerActor(factory: ConnectionFactory, exchangeName: String) extends Actor {
        val connection: Connection = factory.newConnection()
        val channel: Channel = connection.createChannel()

        override def postStop() {
            connection.close()
            super.postStop()
        }

        def receive: Receive = {
            case msg: String =>
                val props = new BasicProperties().builder()
                    .`type`("message")
                    .headers(Map.empty[String, AnyRef].asJava)
                    .build()
                channel.basicPublish(exchangeName, "#", props, msg.getBytes)
        }
    }

    lazy val mockRmqFactory = new MockConnectionFactory
    lazy val mockConnectionProvider = AmqpCachedConnectionProvider(AmqpConnectionFactoryConnectionProvider(mockRmqFactory))
    lazy val materializer: ActorMaterializer = ActorMaterializer()

    test("Consumer should not require rebinding") {
        // Lovely Scala to Java incompatibilities
        val emptyJavaMap = Map.empty[String, AnyRef].asJava

        // Channel settings
        val mockRmqChannel: MockChannel = mockRmqFactory.newConnection.createChannel()
        mockRmqChannel.exchangeDeclare("exchange", "topic", true, false, emptyJavaMap)
        mockRmqChannel.queueDeclare("queue", true, false, false, emptyJavaMap)
        mockRmqChannel.queueBind("queue", "exchange", "#")

        // Source actor that produces to an exchange
        val source1 = system.actorOf(Props(new ProducerActor(mockRmqFactory, "exchange")), "source1")

        // Sink probe that receives messages from a queue in a given channel
        val sink = TestProbe()
        bindActorToQueue(mockRmqChannel, sink.ref, "queue")

        // Send a message to the source and expect it on the sink
        source1 ! "message1"
        assertResult("message1")(sink.receiveN(1, 1.second).head)

        // Kill the source actor. Due to our postStop behaviour, this will close the connection, as akka streams would do
        source1 ! PoisonPill

        // Wait a bit to avoid interferences
        sink.expectNoMessage(500.millis)

        // Create a new producer actor. It creates a new connection through the factory.
        val source2 = system.actorOf(Props(new ProducerActor(mockRmqFactory, "exchange")), "source2")

        //bindActorToQueue(mockRmqChannel, sink.ref, "queue")

        // Wait a bit to avoid interferences
        sink.expectNoMessage(500.millis)

        // Send another message to the source and expect it on the sink
        source2 ! "message2"
        assertResult("message2")(sink.receiveN(1, 1.seconds).head)

        // Expect no extra stuff after the test
        sink.expectNoMessage(500.millis)
    }
}
