package vn.johu.scheduling

import org.quartz.{Job, JobExecutionContext}

import vn.johu.app.ScraperApp

class ScraperSchedulerJob extends Job {

  override def execute(context: JobExecutionContext): Unit = {
    ScraperApp.runScrapers()
  }

}
