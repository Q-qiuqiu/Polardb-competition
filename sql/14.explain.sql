-- using 1731927495 as a seed to the RNG


explain SELECT
    100.00 * SUM(l.l_extendedprice * (1 - l.l_discount)) FILTER (WHERE p.p_type LIKE 'PROMO%')
    / SUM(l.l_extendedprice * (1 - l.l_discount)) AS promo_revenue
FROM
    lineitem l
JOIN
    part p ON l.l_partkey = p.p_partkey
WHERE
    l.l_shipdate >= DATE '1996-07-01'
    AND l.l_shipdate < DATE '1996-08-01';