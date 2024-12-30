-- select
--     nation,
--     o_year,
--     sum(amount)as sum_profit
-- from
--     (
--         select
--             n_name as nation,
--             extract(year from o_orderdate)as o_year,
--             l_extendedprice*(1-l_discount)-ps_supplycost*l_quantity as amount
--         from
--             part,
--             supplier,
--             lineitem,
--             partsupp,
--             orders,
--             nation
--         where
--             s_suppkey =l_suppkey
--             and ps_suppkey=l_suppkey
--             and ps_partkey =l_partkey
--             and p_partkey=l_partkey
--             and o_orderkey=l_orderkey
--             and s_nationkey=n_nationkey
--             and p_name like'%sandy%'
--     )as profit
-- group by
--     nation,
--     o_year
-- order by
--     nation,
--     o_year desc;

set enable_nestloop = off;
SELECT
    n.n_name AS nation,
    EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
    SUM(l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity) AS sum_profit
FROM
    nation n
JOIN supplier s ON n.n_nationkey = s.s_nationkey
JOIN lineitem l ON l.l_suppkey = s.s_suppkey
JOIN partsupp ps ON ps.ps_suppkey = s.s_suppkey AND ps.ps_partkey = l.l_partkey
JOIN part p ON p.p_partkey = l.l_partkey
JOIN orders o ON o.o_orderkey = l.l_orderkey
WHERE
    p.p_name LIKE '%sandy%'
GROUP BY
    n.n_name,
    EXTRACT(YEAR FROM o.o_orderdate)
ORDER BY
    n.n_name,
    o_year DESC;

set enable_nestloop to default;