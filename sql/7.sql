-- select
--     supp_nation,
--     cust_nation,
--     l_year,
--     sum(volume)as revenue
-- from
--     (
--         select
--             n1.n_name as supp_nation,
--             n2.n_name as cust_nation,
--             extract(year from l_shipdate)as l_year,
--             l_extendedprice*(1-l_discount)as volume
--         from
--             supplier,
--             lineitem,
--             orders,
--             customer,
--             nation n1,
--             nation n2
--         where
--             s_suppkey = l_suppkey
--             and o_orderkey=l_orderkey
--             and c_custkey=o_custkey
--             and s_nationkey =n1.n_nationkey
--             and c_nationkey =n2.n_nationkey
--             and(
--                 (n1.n_name ='PERU' and n2.n_name ='VIETNAM')
--                 or(n1.n_name ='VIETNAM'and n2.n_name='PERU')
--             )
--             and l_shipdate between date '1995-01-01'and date '1996-12-31'
--         )as shipping
-- group by
--     supp_nation,
--     cust_nation,
--     l_year
-- order by
--     supp_nation,
--     cust_nation,
--     l_year;

SELECT
    n1.n_name AS supp_nation,
    n2.n_name AS cust_nation,
    EXTRACT(YEAR FROM l_shipdate) AS l_year,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    lineitem l
JOIN
    orders o ON o.o_orderkey = l.l_orderkey
JOIN
    customer c ON c.c_custkey = o.o_custkey
JOIN
    supplier s ON s.s_suppkey = l.l_suppkey
JOIN
    nation n1 ON s.s_nationkey = n1.n_nationkey
JOIN
    nation n2 ON c.c_nationkey = n2.n_nationkey
WHERE
    ((n1.n_name = 'PERU' AND n2.n_name = 'VIETNAM')
    OR (n1.n_name = 'VIETNAM' AND n2.n_name = 'PERU'))
    AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY
    n1.n_name,
    n2.n_name,
    EXTRACT(YEAR FROM l_shipdate)
ORDER BY
    n1.n_name,
    n2.n_name,
    l_year;

