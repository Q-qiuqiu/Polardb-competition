explain SELECT
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice,
    f.total_quantity
FROM
    customer c
JOIN
    orders o ON c.c_custkey = o.o_custkey
JOIN
    mv_filtered_lineitem f ON o.o_orderkey = f.l_orderkey
ORDER BY
    o.o_totalprice DESC,
    o.o_orderdate;