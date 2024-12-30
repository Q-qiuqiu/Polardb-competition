-- using 1731927495 as a seed to the RNG
-- set enable_seqscan = off;
--  select
--      s_name,
--      s_address
--  from
--      supplier,
--      nation
--  where
--      s_suppkey in(
--          select
--              ps_suppkey
--          from
--              partsupp
--          where
--              ps_partkey in(
--                  select
--                      p_partkey
--                  from
--                      part
--                  where
--                      p_name like 'wheat%'
--              )
--              and ps_availqty >(
--                  select
--                      0.5*sum(l_quantity)
--                  from
--                      lineitem
--                  where
--                      l_partkey =ps_partkey
--                      and l_suppkey=ps_suppkey
--                      and l_shipdate >= date'1997-01-01'
--                      and l_shipdate<date '1998-01-01'
--              )
--      )
--      and s_nationkey=n_nationkey
--      and n_name ='JAPAN'
--  order by
--      s_name;
-- set enable_seqscan to default;
WITH wheat_parts AS (
    SELECT p_partkey
    FROM part
    WHERE p_name LIKE 'wheat%'
),
japan_suppliers AS (
    SELECT s.s_suppkey, s.s_name, s.s_address
    FROM supplier s
    JOIN nation n ON s.s_nationkey = n.n_nationkey
    WHERE n.n_name = 'JAPAN'
),
candidate_ps AS (
    SELECT ps.ps_partkey, ps.ps_suppkey, ps.ps_availqty
    FROM partsupp ps
    JOIN wheat_parts wp ON ps.ps_partkey = wp.p_partkey
    JOIN japan_suppliers js ON js.s_suppkey = ps.ps_suppkey
),
lineitem_agg AS (
    SELECT l.l_partkey, l.l_suppkey, 0.5 * SUM(l.l_quantity) AS half_qty_sum
    FROM lineitem l
    JOIN candidate_ps cps ON l.l_partkey = cps.ps_partkey
                         AND l.l_suppkey = cps.ps_suppkey
    WHERE l.l_shipdate >= DATE '1997-01-01'
      AND l.l_shipdate < DATE '1998-01-01'
    GROUP BY l.l_partkey, l.l_suppkey
)
SELECT distinct js.s_name, js.s_address
FROM japan_suppliers js
JOIN candidate_ps cps ON js.s_suppkey = cps.ps_suppkey
LEFT JOIN lineitem_agg la ON la.l_partkey = cps.ps_partkey
                        AND la.l_suppkey = cps.ps_suppkey
WHERE cps.ps_availqty > la.half_qty_sum
ORDER BY js.s_name;
