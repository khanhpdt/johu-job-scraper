package vn.johu.utils

import scala.util.{Failure, Success, Try}

trait TryHelper {

  def getTryResult[T](f: => Try[T], logError: Throwable => Unit): T = {
    f match {
      case Success(value) =>
        value
      case Failure(ex) =>
        logError(ex)
        throw ex
    }
  }

}
