# johu-job-scraper

## About

Scrape jobs from the internet.

## Development guide

## How to run this project locally

1. Use johu-dockerize to start up the infrastructures. Use the docker compose file for starting infrastructure.
2. Run `sbt run`
    - By default, Play will not do anything. To trigger the compilation and startup, run `curl localhost:9000` or anything that can send a request to the application.
    - StartupModule will be executed when the application starts up.

Note:
    - At the moment, we don't start scraping when the app starts up. But in the future, we will start the scheduler ScrapersScheduler to schedule all scrapers. So when you start your app locally, make sure you don't start the scraping. To do that, update the config `johu.scraping.enabled`.

## How to write a new scraper

All scrapers extend JobScrapingCoordinator, which provides a template for the steps to perform when scraping.

Currently, there are the following steps:
    1. Scrape one page
    2. Save the page
    3. Parse the page
    4. Save the parsing result. This is in case we want to check when the parsing does not work as expected.
    5. Publish the parsed job. Currently, we publish to a RabbitMQ queue. 
