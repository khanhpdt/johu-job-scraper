package vn.johu.scheduling

import org.quartz.CronScheduleBuilder.cronSchedule
import org.quartz.JobBuilder.newJob
import org.quartz.TriggerBuilder.newTrigger
import org.quartz.{Job, JobDetail, JobExecutionContext, Trigger}

import vn.johu.app.ScraperApp

class ScraperSchedulerJob extends Job {

  override def execute(context: JobExecutionContext): Unit = {
    ScraperApp.runScrapers()
  }

}

object ScraperSchedulerJob {

  def createNewJob(): (Trigger, JobDetail) = {
    val job = newJob(classOf[ScraperSchedulerJob])
      .withIdentity("ScraperSchedulerJob", "ScraperScheduler")
      .build()

    // run at 3 AM every day
    val cronExp = "0 0 3 * * ?"

    val trigger = newTrigger()
      .withIdentity("ScraperSchedulerJobTrigger", "ScraperSchedulerTrigger")
      .withSchedule(cronSchedule(cronExp))
      .build()

    (trigger, job)
  }

}
