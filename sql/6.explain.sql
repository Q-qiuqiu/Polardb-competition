-- using 1731927495 as a seed to the RNG
explain select
    sum(l_extendedprice *l_discount)as revenue
from
    lineitem
where
    l_shipdate>= date'1993-01-01'
    and l_shipdate<date'1994-01-01'
    and l_discount between 0.06 and 0.08
    and l_quantity< 25;
