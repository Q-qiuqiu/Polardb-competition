select
    sum(l_extendedprice)/7.0 as avg_yearly
from
    lineitem,
    part
where
    p_partkey = l_partkey
    and p_brand ='Brand#35'
    and p_container='JUMBO PKG'
    and l_quantity<(
        select
            0.2* avg(l_quantity)
        from
            lineitem
        where
            l_partkey =p_partkey
    );

-- explain WITH avg_quantity AS (
--     SELECT
--         l_partkey,
--         0.2 * AVG(l_quantity) AS avg_l_quantity
--     FROM
--         lineitem
--     GROUP BY
--         l_partkey
-- )
-- SELECT
--     SUM(l.l_extendedprice) / 7.0 AS avg_yearly
-- FROM
--     lineitem l
-- JOIN
--     part p ON l.l_partkey = p.p_partkey
-- JOIN
--     avg_quantity a ON l.l_partkey = a.l_partkey
-- WHERE
--     p.p_brand = 'Brand#35'
--     AND p.p_container = 'JUMBO PKG'
--     AND l.l_quantity < a.avg_l_quantity;