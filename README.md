# docs-crawler

## Data Model

### jobdef table
* jobtype = key field, identifer for this job type
* start_page = URL of the page from which crawling begins
* single_domain = whether to only crawl links within startpage's domain (1=True)
* subpath = optional; if specified, crawling is limited to this subpath of the domain
* max_pages = maximum number of pages to crawl
* daily = whether to run this job type every day (1=True)

### jobhist table
* job_id = unique identifer for this job run
* jobtype = jobdef.jobtype value for this job type definition
* queued = datetime when this job was added to the queue
* jobstart = datetime when job execution started
* jobend = datetime when job execution ended
* elapsed = total seconds of execution time
* links = total links crawled
* pages = total pages crawled
* missing = total links to missing pages

### crawled table
* job_id = jobhist.job_id of the crawler job in which this page was crawled
* page_url = URL of the page
* crawled = datetime when the page was crawled

### notfound table
* job_id = the jobhist.job_id value for this crawler job
* found = datetime when this broken link was found
* source = the page linked from
* target = the page linked to
* link_text = the display text of the link on the source page