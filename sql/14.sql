-- using 1731927495 as a seed to the RNG
-- select
--     100.00* sum(case
--         when p_type like 'PROMO%'
--             then l_extendedprice*(1-l_discount)
--         else 0
--     end)/sum(l_extendedprice*(1-l_discount))as promo_revenue
-- from
--     lineitem,
--     part
-- where
--     l_partkey =p_partkey
--     and l_shipdate >= date'1996-07-01'
--     and l_shipdate<date'1996-08-01';
-- SELECT
--     100.00 * SUM(CASE
--         WHEN p.p_type LIKE 'PROMO%' THEN l.l_extendedprice * (1 - l.l_discount)
--         ELSE 0
--     END) / SUM(l.l_extendedprice * (1 - l.l_discount)) AS promo_revenue
-- FROM
--     lineitem l
-- JOIN
--     part p ON l.l_partkey = p.p_partkey
-- WHERE
--     l.l_shipdate >= DATE '1996-07-01'
--     AND l.l_shipdate < DATE '1996-08-01';

SELECT
    100.00 * SUM(l.l_extendedprice * (1 - l.l_discount)) FILTER (WHERE p.p_type LIKE 'PROMO%')
    / SUM(l.l_extendedprice * (1 - l.l_discount)) AS promo_revenue
FROM
    lineitem l
JOIN
    part p ON l.l_partkey = p.p_partkey
WHERE
    l.l_shipdate >= DATE '1996-07-01'
    AND l.l_shipdate < DATE '1996-08-01';