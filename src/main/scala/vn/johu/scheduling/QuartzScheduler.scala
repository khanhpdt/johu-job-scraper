package vn.johu.scheduling

import org.quartz.JobBuilder._
import org.quartz.Scheduler
import org.quartz.SimpleScheduleBuilder._
import org.quartz.TriggerBuilder._
import org.quartz.impl.StdSchedulerFactory

import vn.johu.utils.Logging

object QuartzScheduler extends Logging {

  private var scheduler: Scheduler = _

  def init(): Unit = {
    logger.info("Initializing Quartz scheduler...")

    val sf = new StdSchedulerFactory()
    scheduler = sf.getScheduler()
    scheduler.start()

    schedule()
  }

  def close(): Unit = {
    scheduler.shutdown()
  }

  private def schedule(): Unit = {
    logger.info("Scheduling job ScraperSchedulerJob...")

    val job = newJob(classOf[ScraperSchedulerJob])
      .withIdentity("ScraperSchedulerJob", "ScraperScheduler")
      .build()

    val trigger = newTrigger()
      .withIdentity("ScraperSchedulerJobTrigger", "ScraperSchedulerTrigger")
      .startNow()
      .withSchedule(
        simpleSchedule().withIntervalInSeconds(3).withRepeatCount(5)
      )
      .build()

    scheduler.scheduleJob(job, trigger)
  }

}
