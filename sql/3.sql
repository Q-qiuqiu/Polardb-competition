-- using 1731927495 as a seed to the RNG
-- select
--     l_orderkey,
--     sum(l_extendedprice*(1-l_discount))as revenue,
--     o_orderdate,
--     o_shippriority
-- from
--     customer,
--     orders,
--     lineitem
-- where
--     c_mktsegment='BUILDING'
--     and c_custkey = o_custkey
--     and l_orderkey = o_orderkey
--     and o_orderdate <date '1995-03-07'
--     and l_shipdate  >date '1995-03-07'
-- group by
--     l_orderkey,
--     o_orderdate,
--     o_shippriority
-- order by
--     revenue desc,
--     o_orderdate;
select
    l_orderkey,
    sum(l_extendedprice*(1-l_discount))as revenue,
    o_orderdate,
    o_shippriority
from
    customer
    join orders on c_custkey = o_custkey
    join lineitem on l_orderkey = o_orderkey
where
    c_mktsegment='BUILDING'
    and o_orderdate <date '1995-03-07'
    and l_shipdate  >date '1995-03-07'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate;