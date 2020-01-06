# db_dump

Created by running mongodump:
```bash
mongodump --uri="mongodb://localhost:27017/johu-job-scraper"
```

Use mongorestore to restor the DB, e.g.,
```bash
mongorestore dump/johu-job-scraper/ --host=localhost --db=<db_name>
```