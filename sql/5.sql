set enable_nestloop = off;
set enable_bitmapscan = off;

SELECT
    n.n_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
JOIN supplier s ON l.l_suppkey = s.s_suppkey
JOIN nation n ON s.s_nationkey = n.n_nationkey AND c.c_nationkey = s.s_nationkey
JOIN region r ON n.n_regionkey = r.r_regionkey
WHERE
    r.r_name = 'ASIA'
    AND o.o_orderdate >= date '1993-01-01'
    AND o.o_orderdate < date '1994-01-01'
GROUP BY
    n.n_name
ORDER BY
    revenue DESC;

set enable_nestloop to default;
set enable_bitmapscan to default;