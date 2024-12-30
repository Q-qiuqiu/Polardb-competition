
explain SELECT
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