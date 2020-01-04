package vn.johu.utils

object Configs {

  val ScrapingEnabled = "johu.scraping.enabled"
  val ScrapingDelayInMillis = "johu.scraping.delayInMillis"
  val ScrapingJobDetailsDelayInMillis = "johu.scraping.scrapingJobDetailsDelayInMillis"

  val MongoHostUrl = "johu.mongo.host-url"
  val MongoDbName = "johu.mongo.db-name"

  val RabbitMqHost = "johu.rabbitmq.host"
  val RabbitMqPort = "johu.rabbitmq.port"
  val RabbitMqUsername = "johu.rabbitmq.userName"
  val RabbitMqPassword = "johu.rabbitmq.userPassword"
  val RabbitMqQueueName = "johu.rabbitmq.queueName"
  val RabbitMqExchangeName = "johu.rabbitmq.exchangeName"
  val RabbitMqRoutingKey = "johu.rabbitmq.routingKey"

}