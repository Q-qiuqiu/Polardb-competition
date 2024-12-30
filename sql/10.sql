-- -- using 1731927495 as a seed to the RNG
-- select
--     c_custkey,
--     c_name,
--     sum(l_extendedprice*(1-l_discount))as revenue,
--     c_acctbal,
--     n_name,
--     c_address,
--     c_phone,
--     c_comment
-- from
--     customer,
--     orders,
--     lineitem,
--     nation
-- where
--     c_custkey =o_custkey
--     and l_orderkey =o_orderkey
--     and o_orderdate>=date'1993-12-01'and o_orderdate<date'1994-3-01'
--     and l_returnflag='R'
--     and c_nationkey =n_nationkey
-- group by
--     c_custkey,
--     c_name,
--     c_acctbal,
--     c_phone,
--     n_name,
--     c_address,
--     c_comment
-- order by
--     revenue desc;

SELECT 
    c.c_custkey,
    c.c_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    c.c_acctbal,
    n.n_name,
    c.c_address,
    c.c_phone,
    c.c_comment
FROM
    customer c
JOIN 
    orders o ON c.c_custkey = o.o_custkey
JOIN 
    lineitem l ON o.o_orderkey = l.l_orderkey
JOIN 
    nation n ON c.c_nationkey = n.n_nationkey
WHERE
    o.o_orderdate >= DATE '1993-12-01' 
    AND o.o_orderdate < DATE '1994-03-01'
    AND l.l_returnflag = 'R'
GROUP BY
    c.c_custkey,
    c.c_name,
    c.c_acctbal,
    c.c_phone,
    n.n_name,
    c.c_address,
    c.c_comment
ORDER BY
    revenue DESC;