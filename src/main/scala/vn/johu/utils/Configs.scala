package vn.johu.utils

object Configs {

  val ScrapingEnabled = "johu.scraping.enabled"
  val ConcurrentScraping = "johu.scraping.n-concurrent"
  val ScrapingDelay = "johu.scraping.delay"

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