#!/usr/bin/env python3
# ===============================================================================
# LogAnalysis.py
#
# This code will analyze news database and generate text reports
# to extract most popular articles and authors and dates on which
# system experence significant number of errors (%<1)
#
# @author: Nirosh Gunaratne
# @since: 18/02/2018
# @version: 1.0
# ===============================================================================


import psycopg2
import bleach

DBNAME = "news"


class LogAnalysis():

    """Return popular three articles of all time, most popular first."""
    def getPoularArticles(self):
        # Create or overwrite the output file

        articles_file = open('popular_articles.txt', 'w')

        db = psycopg2.connect(database=DBNAME)
        c = db.cursor()
        c.execute("with l as ( \
                        select split_part(path,'/',3) as slug, \
                        split_part(path,'/',2) as resource, \
                        count(id) as views \
                    from log group by path ) \
                    select a.title,l.views from articles as  a \
                    left join l on a.slug = l.slug and \
                    l.resource = 'article' \
                    order by l.views desc \
                    limit 3")
        articles = c.fetchall()
        popular_articles_content = '''\"{title}\" --- {views} views\n'''
        with articles_file:
            for row in articles:
                articles_file.write(
                    popular_articles_content.format(
                        title=row[0], views=row[1]))
        articles_file.close()
        db.close()

        return

    """Return popular author all times, most popular first."""
    def getPoularAuthors(self):
            # Create or overwrite the output file
            authors_file = open('popular_authors.txt', 'w')

            db = psycopg2.connect(database=DBNAME)
            c = db.cursor()
            c.execute("with \
                        a as ( \
                            select au.name,ar.title,ar.slug \
                            from authors as au left join articles as ar \
                            on au.id = ar.author \
                        ), \
                        l as ( \
                            select split_part(path,'/',3) as slug, \
                            split_part(path,'/',2) as resource, \
                            count(id) as views \
                            from log group by path \
                        ), \
                        v as ( \
                            select name,views from a \
                            left join l on a.slug = l.slug \
                            order by views desc ) \
                            select name as author, \
                            sum(views) as views \
                            from v group by name \
                            order by views desc \
                        ")
            authors = c.fetchall()
            popular_authors_content = '''{author} --- {views} views\n'''
            with authors_file:
                for row in authors:
                    authors_file.write(
                        popular_authors_content.format(
                            author=row[0], views=row[1]))
            authors_file.close()
            db.close()
            return

    """Return days with more than 1 percent errors."""
    def getDaysWithErrors(self):
            # Create or overwrite the output file
            error_dates_file = open('error_dates.txt', 'w')

            db = psycopg2.connect(database=DBNAME)
            c = db.cursor()
            c.execute("with l as ( \
                                select date_trunc('day',time)::date as ldate, \
                                    to_number(split_part(status,' ',1),'999') \
                                    as error, id \
                                from log ), \
                            e as ( \
                                select ldate as edate, \
                                    count(error) as errors \
                                    from l  where error > 400 \
                                    group by ldate \
                            ), \
                            r as ( \
                                select ldate as rdate,  count(id) as requests \
                                from l  group by ldate \
                            ),\
                            p as ( \
                                select * , \
                                round ( 100*( \
                                    e.errors::float/ r.requests::float \
                                    )::numeric ,1 ) as t \
                                from r left join e on r.rdate = e.edate \
                            ) \
                            select to_char(p.rdate,'Mon DD, YYYY') as date,\
                                t as errors \
                            from p where t > 1 \
                        ")
            authors = c.fetchall()
            error_dates_content = '''{date} --- {errors} % errors\n'''
            with error_dates_file:
                for row in authors:
                    error_dates_file.write(
                        error_dates_content.
                        format(date=row[0], errors=row[1]))
            error_dates_file.close()
            db.close()

            return

if __name__ == '__main__':
    LogAnalysis = LogAnalysis()
    LogAnalysis.getPoularArticles()
    LogAnalysis.getPoularAuthors()
    LogAnalysis.getDaysWithErrors()
