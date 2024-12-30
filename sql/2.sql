-- using 1731927495 as a seed to the RNG
-- select
--     s_acctbal,
--     s_name,
--     n_name,
--     p_partkey,
--     p_mfgr,
--     s_address,
--     s_phone,
--     s_comment
-- from
--     part,
--     supplier,
--     partsupp,
--     nation,
--     region
-- where
--     p_partkey =ps_partkey
--     and s_suppkey =ps_suppkey
--     and p_size = 28
--     and p_type like '%COPPER'
--     and s_nationkey =n_nationkey
--     and n_regionkey =r_regionkey
--     and r_name='AMERICA'
--     and ps_supplycost=(
--         select
--             min(ps_supplycost)
--         from
--             partsupp,
--             supplier,
--             nation,
--             region
--         where
--             p_partkey = ps_partkey
--             and s_suppkey=ps_suppkey
--             and s_nationkey =n_nationkey
--             and n_regionkey=r_regionkey
--             and r_name ='AMERICA'
--     )
-- order by
--     s_acctbal desc,
--     n_name,
--     s_name,
--     p_partkey;

WITH min_supplycost AS (
    SELECT
        ps_partkey,
        MIN(ps_supplycost) AS min_ps_supplycost
    FROM
        partsupp
        JOIN supplier ON s_suppkey = ps_suppkey
        JOIN nation ON s_nationkey = n_nationkey
        JOIN region ON n_regionkey = r_regionkey
    WHERE
        r_name = 'AMERICA'
    GROUP BY
        ps_partkey
)
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    part
    JOIN partsupp ON p_partkey = ps_partkey
    JOIN supplier ON s_suppkey = ps_suppkey
    JOIN nation ON s_nationkey = n_nationkey
    JOIN region ON n_regionkey = r_regionkey
    JOIN min_supplycost ON partsupp.ps_partkey = min_supplycost.ps_partkey
                        AND partsupp.ps_supplycost = min_supplycost.min_ps_supplycost
WHERE
    p_size = 28
    AND p_type LIKE '%COPPER'
    AND r_name = 'AMERICA'
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey;
