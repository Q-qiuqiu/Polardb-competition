select
    sum( l_extendedprice* ( 1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#45'
        and p_container in ( 'SM CASE', 'SM BOX ' , 'SM PACK ' ,'SM PKG')
        and l_quantity >= 2 and l_quantity <= 12
        and p_size between 1 and 5
        and l_shipmode in ( 'AIR','AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        P_partkey = l_partkey
        and p_brand = 'Brand#53'
        and p_container in ( 'MED BAG","MED BOX','MED PKG','MED PACK ')
        and l_quantity >= 13 and l_quantity <=23
        and p_size between 1 and 10
        and l_shipmode in ( 'AIR','AIR REG' )
        and l_shipinstruct = 'DELIVER IN PERSON '
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#14'
        and p_container in ( 'LG CASE", "LG BOx', 'LG PACK ','LG PKG')
        and l_quantity >= 22 and l_quantity <= 32
        and p_size between 1 and 15
        and l_shipmode in ( 'AIR','AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    );

-- SELECT 
--     SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
-- FROM 
--     lineitem l
-- JOIN 
--     part p ON p.p_partkey = l.l_partkey
-- WHERE 
--     (
--         p.p_brand = 'Brand#45' 
--         AND p.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
--         AND l.l_quantity BETWEEN 2 AND 12
--         AND p.p_size BETWEEN 1 AND 5
--         AND l.l_shipmode IN ('AIR', 'AIR REG')
--         AND l.l_shipinstruct = 'DELIVER IN PERSON'
--     )
--     OR 
--     (
--         p.p_brand = 'Brand#53' 
--         AND p.p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
--         AND l.l_quantity BETWEEN 13 AND 23
--         AND p.p_size BETWEEN 1 AND 10
--         AND l.l_shipmode IN ('AIR', 'AIR REG')
--         AND l.l_shipinstruct = 'DELIVER IN PERSON'
--     )
--     OR 
--     (
--         p.p_brand = 'Brand#14' 
--         AND p.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
--         AND l.l_quantity BETWEEN 22 AND 32
--         AND p.p_size BETWEEN 1 AND 15
--         AND l.l_shipmode IN ('AIR', 'AIR REG')
--         AND l.l_shipinstruct = 'DELIVER IN PERSON'
--     );
