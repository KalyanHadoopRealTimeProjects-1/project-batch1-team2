=========================================================================================================
1. MapReduceTask_1:
➢ Input can be any format like `text, pdf, xml, json`
➢ Partition the given data based on `Country` and `Status`
➢ Output can be any format like `text, pdf, xml, json`

Solutions Steps :---->
Instead of using Partitioner we have to ensure that we create multiple outputs based on Country and status.
Use Custom Input Format and Custom Output Format

=========================================================================================================
2. MapReduceTask_2:
➢ Input can be any format like `text, pdf, xml, json`
➢ Find the top 10 Countries based on their status is `SUCCESS`
➢ Output can be any format like `text, pdf, xml, json`

Solutions Steps: -->
We have use 2 jobs instead of 1
First Job should give output as Country and Count()
Output of First Job will be input for 2nd Job
2nd Job will apply order by logic
SQL Query
Select country, count(1) as cnt
from eventlog
where status = 'SUCCESS'
group by country
order by cnt desc
limit 10;

=========================================================================================================
3. MapReduceTask_3:
➢ Input can be any format like `xml or json`
➢ Find the results where status is `SUCCESS`
➢ Store the result into mysql database `kalyan.eventlog` table

=========================================================================================================
4.MapReduceTask_4:
➢ Read the data from mysql database `kalyan.eventlog` table
➢ Find the top 10 Countries based on their status is `SUCCESS`
➢ Output can be any format like `xml or json`

=========================================================================================================
5.MapReduceTask_5:
➢ Take some `sample image files` as a input format like (.jpg, .png, ...)
➢ Convert the `sample image files` into output format like `.jpg` only


=========================================================================================================













