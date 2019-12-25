package vn.johu.scheduling

import com.typesafe.config.Config
import org.quartz.Scheduler
import org.quartz.impl.StdSchedulerFactory

import vn.johu.utils.{Configs, Logging}

object QuartzScheduler extends Logging {

  private var scheduler: Scheduler = _

  def init(config: Config): Unit = {
    logger.info("Initializing Quartz scheduler...")

    val sf = new StdSchedulerFactory()
    scheduler = sf.getScheduler()
    scheduler.start()

    if (config.getBoolean(Configs.ScrapingEnabled)) {
      scheduleAllJobs()
    } else {
      logger.info("Scraping disabled. Skip scheduling the scrapers.")
    }
  }

  def close(): Unit = {
    logger.info("Closing scheduler...")
    scheduler.shutdown()
  }

  private def scheduleAllJobs(): Unit = {
    logger.info("Scheduling job ScraperSchedulerJob...")

    val (trigger, job) = ScraperSchedulerJob.createNewJob()
    scheduler.scheduleJob(job, trigger)

    logger.info(s"Scheduled job [$job] with trigger [$trigger]")
  }

}
