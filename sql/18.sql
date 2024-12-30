-- -- using 1731927495 as a seed to the RNG
-- select 
--   c_name, 
--   c_custkey, 
--   o_orderkey, 
--   o_orderdate, 
--   o_totalprice, 
--   sum(l_quantity) 
-- from 
--   customer, 
--   orders, 
--   lineitem 
-- where 
--   o_orderkey in (
--     select 
--       l_orderkey 
--     from 
--       lineitem 
--     group by 
--       l_orderkey 
--     having 
--       sum(l_quantity) > 315
--   ) 
--   and c_custkey = o_custkey 
--   and o_orderkey = l_orderkey 
-- group by 
--   c_name, 
--   c_custkey, 
--   o_orderkey, 
--   o_orderdate, 
--   o_totalprice 
-- order by 
--   o_totalprice desc, 
--   o_orderdate;


SET work_mem = '256MB';
CREATE MATERIALIZED VIEW mv_filtered_lineitem AS
SELECT
    l_orderkey,
    SUM(l_quantity) AS total_quantity
FROM
    lineitem
GROUP BY
    l_orderkey
HAVING
    SUM(l_quantity) > 315;

-- 主查询，利用物化视图加速
SELECT
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice,
    SUM(l.l_quantity) AS total_quantity
FROM
    customer c
JOIN
    orders o ON c.c_custkey = o.o_custkey
JOIN
    mv_filtered_lineitem f ON o.o_orderkey = f.l_orderkey
JOIN
    lineitem l ON o.o_orderkey = l.l_orderkey
GROUP BY
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice
ORDER BY
    o.o_totalprice DESC,
    o.o_orderdate;
SET work_mem='1024MB';