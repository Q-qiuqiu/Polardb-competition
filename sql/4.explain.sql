-- using 1731927495 as a seed to the RNG


explain SELECT 
    o.o_orderpriority,
    COUNT(*) AS order_count
FROM 
    orders o
JOIN 
    lineitem l ON o.o_orderkey = l.l_orderkey
WHERE 
    o.o_orderdate >= DATE '1994-02-01' 
    AND o.o_orderdate < DATE '1994-05-01'
    AND l.l_commitdate < l.l_receiptdate
GROUP BY 
    o.o_orderpriority
ORDER BY 
    o.o_orderpriority;