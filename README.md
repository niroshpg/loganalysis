# loganalysis
This project demonstrates how to analyze news database and infer insight on
popular content and errors encountered by the system


Preconditions:
This project started with exiting vagrant environment with Python and
PostgreSQL Database server already install. Also load exiting news.sql database
to database server and content can be accessed by vagrant ssh shell.

How to run:
1. Copy content to vagrant shared folder

2. Execute command:
$python LogAnalysis.py

3. Reports will be generated with filenames:
i) "popular_articles.txt",
ii) "popular_authors.txt" and
iii) error_dates.txt
