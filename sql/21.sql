-- using 1731927495 as a seed to the RNG
-- select
--     s_name,
--     count(*)as numwait
-- from
--     supplier,
--     lineitem l1,
--     orders,
--     nation
-- where
--     s_suppkey = l1.l_suppkey
--     and o_orderkey = l1.l_orderkey
--     and o_orderstatus='F'
--     and l1.l_receiptdate >l1.l_commitdate
--     and exists(
--         select*
--         from
--             lineitem l2
--         where
--             l2.l_orderkey= l1.l_orderkey
--             and l2.l_suppkey<>l1.l_suppkey
--     )
--     and not exists(
--         select*
--         from
--             lineitem l3
--         where
--             l3.l_orderkey= l1.l_orderkey
--             and l3.l_suppkey<>l1.l_suppkey
--             and l3.l_receiptdate >l3.l_commitdate
--     )
--     and s_nationkey =n_nationkey
--     and n_name ='RUSSIA'
-- group by
--     s_name
-- order by
--     numwait desc,
--     s_name;
SELECT
    s.s_name,
    COUNT(*) AS numwait
FROM
    supplier s
JOIN
    lineitem l1 ON s.s_suppkey = l1.l_suppkey
JOIN
    orders o ON o.o_orderkey = l1.l_orderkey
JOIN
    nation n ON s.s_nationkey = n.n_nationkey
WHERE
    o.o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT 1
        FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey
        AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT 1
        FROM lineitem l3
        WHERE l3.l_orderkey = l1.l_orderkey
        AND l3.l_suppkey <> l1.l_suppkey
        AND l3.l_receiptdate > l3.l_commitdate
    )
    AND n.n_name = 'RUSSIA'
GROUP BY
    s.s_name
ORDER BY
    numwait DESC,
    s.s_name;