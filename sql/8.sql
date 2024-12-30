set enable_nestloop = off;

-- SELECT
--     EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
--     SUM(
--         CASE
--             WHEN n2.n_name = 'VIETNAM' THEN l.l_extendedprice * (1 - l.l_discount)
--             ELSE 0
--         END
--     ) / SUM(l.l_extendedprice * (1 - l.l_discount)) AS mkt_share
-- FROM
--     part p
--     JOIN lineitem l ON p.p_partkey = l.l_partkey
--     JOIN supplier s ON l.l_suppkey = s.s_suppkey
--     JOIN partsupp ps ON p.p_partkey = ps.ps_partkey AND s.s_suppkey = ps.ps_suppkey
--     JOIN orders o ON l.l_orderkey = o.o_orderkey
--     JOIN customer c ON o.o_custkey = c.c_custkey
--     JOIN nation n1 ON c.c_nationkey = n1.n_nationkey
--     JOIN region r ON n1.n_regionkey = r.r_regionkey
--     JOIN nation n2 ON s.s_nationkey = n2.n_nationkey
-- WHERE
--     r.r_name = 'ASIA'
--     AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
--     AND p.p_type = 'ECONOMY BRUSHED BRASS'
-- GROUP BY
--     EXTRACT(YEAR FROM o.o_orderdate)
-- ORDER BY
--     o_year;
WITH asian_nations AS (
    SELECT n.n_nationkey
    FROM nation n
    JOIN region r ON n.n_regionkey = r.r_regionkey
    WHERE r.r_name = 'ASIA'
),
filtered_parts AS (
    SELECT p_partkey
    FROM part
    WHERE p_type = 'ECONOMY BRUSHED BRASS'
),
filtered_orders AS (
    SELECT o_orderkey, EXTRACT(YEAR FROM o_orderdate) AS o_year
    FROM orders
    WHERE o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
),
eligible_data AS (
    SELECT 
        o.o_year,
        l.l_extendedprice,
        l.l_discount,
        n2.n_name
    FROM filtered_parts p
    JOIN lineitem l ON p.p_partkey = l.l_partkey
    JOIN supplier s ON l.l_suppkey = s.s_suppkey
    JOIN nation n2 ON s.s_nationkey = n2.n_nationkey
    JOIN partsupp ps ON p.p_partkey = ps.ps_partkey AND s.s_suppkey = ps.ps_suppkey
    JOIN filtered_orders o ON o.o_orderkey = l.l_orderkey
    JOIN customer c ON o.o_orderkey = c.c_custkey
    JOIN asian_nations an ON c.c_nationkey = an.n_nationkey
)
SELECT
    o_year,
    SUM(CASE WHEN n_name = 'VIETNAM' THEN l_extendedprice*(1-l_discount) ELSE 0 END)
    / SUM(l_extendedprice*(1-l_discount)) AS mkt_share
FROM eligible_data
GROUP BY o_year
ORDER BY o_year;
set enable_nestloop to default;