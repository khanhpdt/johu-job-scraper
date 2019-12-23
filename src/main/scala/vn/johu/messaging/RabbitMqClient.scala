package vn.johu.messaging

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import com.typesafe.config.Config
import io.circe.Encoder
import io.circe.syntax._

import vn.johu.utils.{Configs, Logging, TryHelper}

object RabbitMqClient extends Logging with TryHelper {

  private var connection: Connection = _
  private var channel: Channel = _

  private var exchangeName: String = _
  private var routingKey: String = _

  def init(config: Config): Unit = {
    val factory = new ConnectionFactory
    factory.setUsername(config.getString(Configs.RabbitMqUsername))
    factory.setPassword(config.getString(Configs.RabbitMqPassword))
    factory.setHost(config.getString(Configs.RabbitMqHost))
    factory.setPort(config.getInt(Configs.RabbitMqPort))
    factory.setAutomaticRecoveryEnabled(true)

    connection = retry(3, Some(10.seconds), Some("Connect to RabbitMQ")) {
      factory.newConnection
    }
    channel = connection.createChannel

    exchangeName = config.getString(Configs.RabbitMqExchangeName)
    channel.exchangeDeclare(exchangeName, "direct", true)

    val queueName = config.getString(Configs.RabbitMqQueueName)
    channel.queueDeclare(queueName, true, false, false, null)

    routingKey = config.getString(Configs.RabbitMqRoutingKey)
    channel.queueBind(queueName, exchangeName, routingKey)
  }

  def close(): Unit = {
    logger.info("Closing RabbitMQ client...")
    Try {
      channel.close()
      connection.close()
    } match {
      case Success(_) =>
        logger.info("Successfully closed RabbitMQ client.")
      case Failure(ex) =>
        logger.error("Error when closing RabbitMQ client.", ex)
    }
  }

  def publishAsync[T: Encoder](message: T)(implicit ec: ExecutionContext): Future[Unit] = {
    val json = message.asJson.toString()
    logger.trace(s"Received new message: $json")

    val result = Future {
      channel.basicPublish(exchangeName, routingKey, null, json.getBytes)
    }

    result.onComplete {
      case Failure(exception) =>
        logger.error("Failed to publish message to RabbitMQ.", exception)
      case Success(_) =>
    }

    result
  }

}
