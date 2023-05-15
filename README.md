# RedisOlapClient
A scala program to perform ssb benchmark queries on redis

# Redis version
This program needs redis stack with redis 7, which is currently the edge version.
````docker run -p6379:6379 redis/redis-stack:edge````

# Queries
Original queries based on the ones found [here](https://github.com/nuko-yokohama/ssb-postgres/blob/master/explain.sql).
## Q1.1
### Original
```sql
select sum(lo_extendedprice*lo_discount) as revenue   
from lineorder, date   
where lo_orderdate = d_datekey
  and d_year = 1993
  and lo_discount between 1 and 3
  and lo_quantity < 25;
```

### Redis
#### Structure of fields:
In redis, everything is stored as a so-called hash (a key-value store).
The key to the hash consists of the database name and the value of the primary kex field(s) in SQL.

##### lineorder:
In the lineorder table, there are two primary keys: "lo_orderkey" and "lo_linenumber".
So the structure of the key in redis is: ```lineorder:lo_orderkey:lo_linenumber```
e.g. ```lineorder:32:2```

| key              | value    |
|------------------|----------|
| lo_orderkey      | 32       |
| lo_linenumber    | 2        |
| lo_custkey       | 5954     |
| lo_partkey       | 197921   |
| lo_suppkey       | 566      |
| lo_orderdate     | 19960404 |
| lo_orderpriority | 3-MEDIUM |
| lo_shippriority  | 0        |
| lo_quantity      | 32       |
| lo_extendedprice | 6460544  |
| lo_ordtotalprice | 11999065 |
| lo_discount      | 2        |
| lo_revenue       | 6331333  |
| lo_supplycost    | 121135   |
| lo_tax           | 0        |
| lo_commitdate    | 19960626 |
| lo_shipmod       | AIR      |

##### date:
The date table only has one primary key, the "d_datekey".
so it can look like this: ```date:19960111```


| key                | value            |
|--------------------|------------------|
| d_datekey          | 19960111         |
| d_date             | January 11, 1996 |
| d_dayofweek        | Friday           |
| d_month            | January          |
| d_year             | 1996             |
| d_yearmonthnum     | 199601           |
| d_yearmonth        | Jan1996          |
| d_daynuminweek     | 6                |
| d_daynuminmonth    | 11               |
| d_daynuminyear     | 11               |
| d_monthnuminyear   | 1                |
| d_weeknuminyear    | 2                |
| d_sellingseason    | Winter           |
| d_lastdayinweekfl  | 0                |
| d_lastdayinmonthfl | 1                |
| d_holidayfl        | 0                |
| d_weekdayfl        | 1                |

#### Approach:
1. Get all keys of the date hashes where the field d_year has the value 1993
2. Get all documents of the lineorder hashes where the lo_discount is between 1 and 3 and the lo_quantity < 25
3. Filter all lineoder documents based on if their lo_orderdate is present in the keys of 1.
4. Sum up all (lo_extendedprice*lo_discount) of the lineorder documents





## Q1.2
### Original
```sql
select sum(lo_extendedprice*lo_discount) as revenue   
from lineorder, date   
where lo_orderdate = d_datekey
and d_yearmonthnum = 199401
and lo_discount between 4 and 6
and lo_quantity between 26 and 35;
```

### Approach:
Very similar to Q1.1:

1. Get all keys of the date hashes where the field d_yearmonthnum has the value 199401
2. Get all documents of the lineorder hashes where the lo_discount is between 4 and 6 and the lo_quantity between 26 and 35
3. Filter all lineoder documents based on if their lo_orderdate is present in the keys of 1.
4. Sum up all (lo_extendedprice*lo_discount) of the lineorder documents

To work, the index must contain d_yearmonthnum.
``` TERMINAl
FT.ALTER date-index SCHEMA ADD d_yearmonthnum NUMERIC
```
## Q1.3
### Original
```sql
select sum(lo_extendedprice*lo_discount) as revenue   
from lineorder, date   
where lo_orderdate = d_datekey
and d_weeknuminyear = 6
and d_year = 1994
and lo_discount between 5 and 7
and lo_quantity between 26 and 35;
```
### Approach:
See Q1.1 and Q1.2

To work, the index must contain d_weeknuminyear.
``` TERMINAl
FT.ALTER date-index SCHEMA ADD d_weeknuminyear NUMERIC 
```
## Q2.1
### Original
```sql
select sum(lo_revenue), d_year, p_brand1
from lineorder, date, part, supplier
where lo_orderdate = d_datekey
and lo_partkey = p_partkey
and lo_suppkey = s_suppkey
and p_category = 'MFGR#12'
and s_region = 'AMERICA'
group by d_year, p_brand1
order by d_year, p_brand1;
```
TODO: Check if faster when indexs are sortable
To work, some more indexes have to be created and existing ones have to be extended:
``` TERMINAl
FT.ALTER lineorder-index SCHEMA ADD lo_partkey NUMERIC 
FT.ALTER lineorder-index SCHEMA ADD lo_suppkey NUMERIC 
```
``` TERMINAL
FT.CREATE part-index PREFIX 1 "part:" SCHEMA p_category TEXT
```

``` TERMINAL
FT.CREATE supplier-index PREFIX 1 "supplier:" SCHEMA s_region TEXT
```

## Q2.2
### Original
```sql
select sum(lo_revenue), d_year, p_brand1
from lineorder, date, part, supplier
where lo_orderdate = d_datekey
and lo_partkey = p_partkey
and lo_suppkey = s_suppkey
and p_brand1 between 'MFGR#2221' and 'MFGR#2228'
and s_region = 'ASIA'
group by d_year, p_brand1
order by d_year, p_brand1;
```
To work, the part index has to be altered:
```` TERMINAL
FT.ALTER part-index SCHEMA ADD p_brand1 TEXT 
````


## Q2.3
### Original
```sql
select sum(lo_revenue), d_year, p_brand1
from lineorder, date, part, supplier
where lo_orderdate = d_datekey
and lo_partkey = p_partkey
and lo_suppkey = s_suppkey
and p_brand1 = 'MFGR#2221'
and s_region = 'EUROPE'
group by d_year, p_brand1
order by d_year, p_brand1;
```

## Q3.1
### Original
```sql
select c_nation, s_nation, d_year, sum(lo_revenue) as revenue
from customer, lineorder, supplier, date
where lo_custkey = c_custkey
and lo_suppkey = s_suppkey
and lo_orderdate = d_datekey
and c_region = 'ASIA'
and s_region = 'ASIA'
and d_year >= 1992
and d_year <= 1997
group by c_nation, s_nation, d_year
order by d_year asc, revenue desc;
```

## Q3.2
### Original
```sql
select c_city, s_city, d_year, sum(lo_revenue) as revenue
from customer, lineorder, supplier, date
where lo_custkey = c_custkey
and lo_suppkey = s_suppkey
and lo_orderdate = d_datekey
and c_nation = 'UNITED STATES'
and s_nation = 'UNITED STATES'
and d_year >= 1992
and d_year <= 1997
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
```

## Q3.3
### Original
```sql
select c_city, s_city, d_year, sum(lo_revenue) as revenue
from customer, lineorder, supplier, date
where lo_custkey = c_custkey
and lo_suppkey = s_suppkey
and lo_orderdate = d_datekey
and (c_city='UNITED KI1' or c_city='UNITED KI5')
and (s_city='UNITED KI1' or s_city='UNITED KI5')
and d_year >= 1992
and d_year <= 1997
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
```

## Q3.4
### Original
```sql
select c_city, s_city, d_year, sum(lo_revenue) as revenue
from customer, lineorder, supplier, date
where lo_custkey = c_custkey
and lo_suppkey = s_suppkey
and lo_orderdate = d_datekey
and (c_city='UNITED KI1' or c_city='UNITED KI5')
and (s_city='UNITED KI1' or s_city='UNITED KI5')
and d_yearmonth = 'Dec1997'
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
```

## Q4.1
### Original
```sql
select d_year, c_nation, sum(lo_revenue - lo_supplycost) as profit
from date, customer, supplier, part, lineorder
where lo_custkey = c_custkey
and lo_suppkey = s_suppkey
and lo_partkey = p_partkey
and lo_orderdate = d_datekey
and c_region = 'AMERICA'
and s_region = 'AMERICA'
and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by d_year, c_nation
order by d_year, c_nation;
```

## Q4.2
### Original
```sql
select d_year, s_nation, p_category, sum(lo_revenue - lo_supplycost) as profit
from date, customer, supplier, part, lineorder
where lo_custkey = c_custkey
and lo_suppkey = s_suppkey
and lo_partkey = p_partkey
and lo_orderdate = d_datekey
and c_region = 'AMERICA'
and s_region = 'AMERICA'
and (d_year = 1997 or d_year = 1998)
and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by d_year, s_nation, p_category
order by d_year, s_nation, p_category;
```

## Q4.3
### Original
```sql
select d_year, s_city, p_brand1, sum(lo_revenue - lo_supplycost) as profit
from date, customer, supplier, part, lineorder
where lo_custkey = c_custkey
and lo_suppkey = s_suppkey
and lo_partkey = p_partkey
and lo_orderdate = d_datekey
and c_region = 'AMERICA'
and s_nation = 'UNITED STATES'
and (d_year = 1997 or d_year = 1998)
and p_category = 'MFGR#14'
group by d_year, s_city, p_brand1
order by d_year, s_city, p_brand1;
```