package vn.johu.utils

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

trait TryHelper extends Logging {

  def getTryResult[T](f: => Try[T], logError: Throwable => Unit): T = {
    f match {
      case Success(value) =>
        value
      case Failure(ex) =>
        logError(ex)
        throw ex
    }
  }

  @tailrec
  final def retry[T](
    nTry: Int,
    sleepInterval: Option[Duration] = Some(30.seconds),
    opName: Option[String] = None
  )(op: => T): T = {
    Try(op) match {
      case Success(result) =>
        opName.foreach(name => logger.info(s"Operation [$name] succeeds."))
        result
      case Failure(_) if nTry > 0 =>
        logger.info(s"Failed to execute operation [${opName.getOrElse("")}]. #tries left=${nTry - 1}. Retrying...")
        sleepInterval.foreach(interval => Thread.sleep(interval.toMillis))
        retry(nTry - 1, sleepInterval)(op)
      case Failure(e) =>
        logger.info(s"Failed to execute function. All $nTry retries done. Aborting...")
        throw e
    }
  }

}
