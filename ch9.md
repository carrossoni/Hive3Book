# 9 OLAP and Hive

## Materialized Views

```sql
create materialized view cube_cancellations_by_time 
as 
select 
   year, 
   month, 
   dayofmonth,
   sum(cancelled)     as sum_cancelled, 
   round(100/sum(1)*sum(cancelled),0)        
                    as pct_cancelled, 
   count(1)         as sum_total_flights
from 
   flights
group by 
   year, 
   month, 
   dayofmonth;
```


```sql
show materialized views;
```

```sql
describe formatted cube_cancellations_by_time;
```

```sql
select 
   sum_cancelled, 
   pct_cancelled, 
   sum_total_flights
from 
   cube_cancellations_by_time
where 
   year = 1995 and 
   month = 1 and 
   dayofmonth = 1;
```

```sql
alter materialized view cube_cancellations_by_time 
   disable rewrite;
```

```sql
select 
   month, 
   dayofmonth, 
   sum((`cancelled`)) 
from 
   flights
group by 
   month, 
   dayofmonth;
```

```sql
ALTER materialized view cube_cancellations_by_time 
enable rewrite;
```

```sql
select 
   month, 
   dayofmonth, 
   sum((`cancelled`)) 
from 
   flights
group by 
   month, 
   dayofmonth;
```

```sql
create materialized view cube_cancellations_by_airline_time 
partitioned on (year)
as 
select 
   year, 
   month, 
   dayofmonth, 
   uniquecarrier, 
   sum(cancelled)     as sum_cancelled, 
   round(100/sum(1)*sum(cancelled),0)     as pct_cancelled, 
   count(1)      as sum_total_flights
from 
   flights
group by 
   year, 
   month, 
   dayofmonth,
   uniquecarrier;
```

```sql
describe formatted cube_cancellations_by_airline_time;
```


## Advanced Aggregation 

```sql
with avg_day 
as 
( 
select 
   concat(year, lpad(month,2,'0'),
   lpad(dayofmonth,2,'0'))        as day, 
     sum(nvl(cancelled))           as val 
from flights 
  where year = 1995 
group by 
  concat(year, lpad(month,2,'0'),
  lpad(dayofmonth,2,'0')) 
) 
select 
  *,  
  round( avg(val) over 
   (order by tag rows between 6 preceding and current row), 0) as 7day_avg 
from 
   avg_day;
```

```sql
Select * from 
  ( 
    with rank_route AS ( 
      select
        origin, 
        dest, 
        SUM(nvl(cancelled)) AS sum_can 
      from 
         flights 
      where 
         YEAR = 1995 
      GROUP BY 
         origin, 
         dest 
    ) 
    SELECT 
      *, 
      RANK() OVER (PARTITION BY origin ORDER BY sum_can desc 
      ) AS RANK 
    FROM 
      rank_route 
  ) t 
WHERE 
  RANK < 4 
```

```sql
select  
origin, 
dest, 
count(1)as sum_total_flights
from 
   flights
where 
   year = 1995
group by 
origin, 
dest 
grouping sets (
(origin,dest),
(origin),
()
)
having 
origin is null or 
origin = 'SFO';
```

```sql
select  
origin, 
dest, 
count(1)as sum_total_flights,
  grouping__id
from 
   flights
where 
   year = 1995
group by 
origin, 
dest 
grouping sets (
(origin,dest),
(origin),
()
)
having 
origin is null or 
origin = 'SFO';
```

```sql
select 
year,
origin, 
dest, 
  sum(cancelled) as sum_cancelled
from 
   flights
group by 
   year, 
   origin, 
   dest 
   with cube;
```

```sql
select 
year,
origin, 
dest, 
  sum(cancelled) as sum_cancelled
from 
   flights
group by 
  year, 
  origin, 
  dest 
  with rollup;
```

```sql
create materialized view cube_origin_dest 
as
select 
   origin, 
   dest, 
   sum(cancelled)     as sum_cancelled, 
   count(1)           as sum_total_flights
from 
   flights
group by 
   origin, 
   dest 
   with cube;
```

```sql
select * from cube_origin_dest;
```


## Data sketches 

```sql
show functions like ‘ds_freq%’;
```

```sql
describe function extended;
```

```sql
create table flights_frq_route_sketch 
(cancelled int, route binary);
```

```sql
insert into flights_frq_route_sketch 
select 
   cancelled, 
   ds_freq_sketch(concat(origin,'-', dest), 4096 ) as route
from 
   flights
group by 
   cancelled;
```

```sql
select ds_freq_items(route) 
from flights_frq_route_sketch
where cancelled = 1;
```

```sql
create table flights_hll_sketch 
as
select 
ds_hll_sketch( 
              cast(concat(uniquecarrier,flightnum) as string) 
              ) as flightnum_sk
from 
  flights;
```

```sql
select 
      ds_hll_estimate(flightnum_sk)
from 
   airlinedata.flights_hll_sketch;
```

```sql
select 
   count(distinct(cast(concat(uniquecarrier,flightnum) as string))) 
from 
    flights;
```

```sql
create table flights_qt_sketch
as
select year, 
       uniquecarrier, 
       count(1) as sum_flights,
       ds_quantile_doubles_sketch(cast(arrdelay + 1 as double))         
          as sk_arrdelay
from 
   flights
where 
   arrdelay > 0
group by 
   year, 
   uniquecarrier;
```

```sql
select 
  year,  
ds_quantile_doubles_cmf(sk_arrdelay, 25, 50, 75, 100 ) 
from  
 flights_qt_sketch
where 
   uniquecarrier = 'AA' and 
   year = 1995
order by 1;
```

```sql
select 
   year,  
   ds_quantile_doubles_cdf(
                 sk_arrdelay,10,20,30,40,50,60,70,80,90,100) 
from  
  flights_qt_sketch
where 
uniquecarrier = 'AA' and 
year = 1995
order by 1
```

```sql
create table flights_frq_route_sketch(
    year int, 
cancelled int, 
route binary);
```

```sql
insert into flights_frq_route_sketch 
select 
   year, 
   cancelled, 
   ds_freq_sketch(concat(origin,'-',dest), 4096 )
from 
   flights
where 
   year in (1995,1996,1997)
group by 
   year, 
   cancelled;
```

```sql
select 
   ds_freq_frequent_items( (route)) 
from 
   flights_frq_route_sketch 
where 
   cancelled = 1 and 
   year in (1995);
```

```sql
select 
   ds_freq_frequent_items( (route))  
from 
   flights_frq_route_sketch 
where 
   cancelled = 1 and 
   year in (1996);
```

```sql
select 
   ds_freq_frequent_items( (route))  
from 
   flights_frq_route_sketch 
where 
   cancelled = 1 and 
   year in (1997); 
```

```sql
select 
   ds_freq_frequent_items( ds_freq_union(route)) 
from 
   flights_frq_route_sketch 
where 
   cancelled = 1;
```





