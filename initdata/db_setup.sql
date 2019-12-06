CREATE TABLE jobdef (jobtype CHAR(20) PRIMARY KEY, start_page VARCHAR(120), single_domain BIT, subpath VARCHAR(120), max_pages INT, daily BIT);
CREATE TABLE jobhist (job_id INT IDENTITY(1,1) PRIMARY KEY, jobtype CHAR(20), queued DATETIME2, jobstart DATETIME2, jobend DATETIME2, elapsed INT, links INT, pages INT, missing INT);
CREATE TABLE crawled (job_id INT, page_url VARCHAR(120), crawled DATETIME2);
CREATE TABLE notfound (job_id INT, found DATETIME2, source VARCHAR(120), target VARCHAR(120), link_text VARCHAR(120));
