-- using 1731927495 as a seed to the RNG
-- select
--     p_brand,
--     p_type,
--     p_size,
--     count(distinct ps_suppkey)as supplier_cnt
-- from
--     partsupp,
--     part
-- where
--     p_partkey =ps_partkey
--     and p_brand<>'Brand#13'
--     and p_type not like 'ECONOMY BRUSHED%'
--     and p_size in(37,49,46,26,11,41,13,21)
--     and ps_suppkey not in(
--         select
--             s_suppkey
--         from
--             supplier
--         where
--             s_comment like'%Customer%complaints%'
--     )
-- group by
--     p_brand,
--     p_type,
--     p_size
-- order by
--     supplier_cnt desc,
--     p_brand,
--     p_type,
--     p_size;

SELECT
    p.p_brand,
    p.p_type,
    p.p_size,
    COUNT(DISTINCT ps.ps_suppkey) AS supplier_cnt
FROM
    partsupp ps
JOIN 
    part p ON p.p_partkey = ps.ps_partkey
LEFT JOIN 
    supplier s ON ps.ps_suppkey = s.s_suppkey AND s.s_comment LIKE '%Customer%complaints%'
WHERE
    p.p_brand <> 'Brand#13'
    AND p.p_type NOT LIKE 'ECONOMY BRUSHED%'
    AND p.p_size IN (37, 49, 46, 26, 11, 41, 13, 21)
    AND s.s_suppkey IS NULL  -- Only include suppliers not in the complaints list
GROUP BY
    p.p_brand,
    p.p_type,
    p.p_size
ORDER BY
    supplier_cnt DESC,
    p.p_brand,
    p.p_type,
    p.p_size;
