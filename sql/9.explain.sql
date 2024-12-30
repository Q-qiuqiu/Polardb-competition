-- using 1731927495 as a seed to the RNG
explain SELECT
    n.n_name AS nation,
    EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
    SUM(l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity) AS sum_profit
FROM
    lineitem l
JOIN
    supplier s ON s.s_suppkey = l.l_suppkey
JOIN
    partsupp ps ON ps.ps_suppkey = l.l_suppkey AND ps.ps_partkey = l.l_partkey
JOIN
    part p ON p.p_partkey = l.l_partkey
JOIN
    orders o ON o.o_orderkey = l.l_orderkey
JOIN
    nation n ON n.n_nationkey = s.s_nationkey
WHERE
    p.p_name LIKE '%sandy%' -- 可以优化为 LIKE 'sandy%' 如果可能
GROUP BY
    n.n_name,
    EXTRACT(YEAR FROM o.o_orderdate)
ORDER BY
    n.n_name,
    o_year DESC;