{\rtf1\ansi\ansicpg1252\cocoartf2761
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww33400\viewh21000\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs28 \cf0 \'97QS Ranking Queries \
\
CREATE EXTERNAL TABLE IF NOT EXISTS `demo_db.university_ranking_csv`(\
  `university` string, \
  `year` string, \
  `rank_display` string, \
  `score` string, \
  `link` string, \
  `country` string, \
  `city` string, \
  `region` string, \
  `logo` string, \
  `type` string, \
  `research_output` string, \
  `student_faculty_ratio` string, \
  `international_students` string, \
  `size` string, \
  `faculty_count` string)\
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \
WITH SERDEPROPERTIES ("separatorChar" = ",", "escapeChar" = "\\\\", "quoteChar"='\\"') \
LOCATION\
  's3://customer-review-csv/university_ranking/csv/'\
TBLPROPERTIES ("skip.header.line.count"="1") \
\
SELECT * \
FROM "AwsDataCatalog"."demo_db"."university_ranking_csv"\
WHERE university like '%Nanyang Technological University%'\
ORDER BY year, rank_display;\
\
 SELECT coalesce(try(cast(split_part(rank_display,'-',1) as int)), 9999) as n_rank\
FROM "AwsDataCatalog"."demo_db"."university_ranking_csv" where coalesce(try(cast(split_part(rank_display,'-',1) as int)), 9999) < 5 ;\
\
 SELECT coalesce(try(cast(split_part(rank_display,'-',1) as int)), 9999) as n_rank\
FROM "AwsDataCatalog"."demo_db"."university_ranking_csv" offset 400 limit 10;\
\
SELECT *\
FROM\
(SELECT university, year, \
       rank_display, \
       coalesce(try(cast(split_part(rank_display,'-',1) as int)), 9999) as n_rank,\
       score, country, city, region, type,\
       research_output, student_faculty_ratio, international_students,\
       size, faculty_count\
FROM "demo_db"."university_ranking_csv")\
WHERE n_rank < 6\
order by year, n_rank \
\
 SELECT regexp_replace(international_students,'[.,]','') as international_students,\
       regexp_replace(faculty_count,'[.,]','') as faculty_count\
FROM "demo_db"."university_ranking_csv" \
WHERE country = 'Norway'; \
\
\
 SELECT regexp_replace(international_students,'[.,]','') as international_students,\
       regexp_replace(faculty_count,'[.,]','') as faculty_count\
FROM "learn_by_doing"."university_ranking_csv" \
WHERE country = 'Norway'; }

 CREATE OR REPLACE VIEW university_ranking_view AS
SELECT university,
       COALESCE(TRY(CAST(year AS int)),9999) AS year, 
       rank_display, 
       COALESCE(TRY(CAST(split_part(rank_display,'-',1) AS int)),9999) AS n_rank,
       COALESCE(TRY(CAST(score AS double)),-1) AS score, 
       country, city, region, type,
       research_output, 
       COALESCE(TRY(CAST(student_faculty_ratio AS double)),-1) AS student_faculty_ratio,
       COALESCE(TRY(CAST(regexp_replace(international_students,'[.,]','') AS int)),-1) as international_students,
       size, 
       COALESCE(TRY(CAST(regexp_replace(faculty_count,'[.,]','') AS int)),-1) as faculty_count
FROM "demo_db"."university_ranking_csv"
order by year, n_rank; 