select
    cntrycode,
    count(*)as numcust,
    sum(c_acctbal)as totacctbal
from
    (
        select
            substring(c_phone from 1 for 2)as cntrycode,
            c_acctbal
        from
            customer
        where
            substring(c_phone from 1 for 2)in('32','24','40','26','30','28','31')
            and c_acctbal>(
                select
                    avg(c_acctbal)
                from
                    customer
                where
                    c_acctbal >0.00
                    and substring(c_phone from 1 for 2)in('32','24','40','26','30','28','31')
            )
            and not exists(
                select
                *
                from
                    orders
                where
                    o_custkey = c_custkey
            )
    )as custsale
group by
    cntrycode
order by
    cntrycode;


-- WITH avg_balance AS (
--     SELECT
--         AVG(c_acctbal) AS avg_c_acctbal
--     FROM
--         customer
--     WHERE
--         c_acctbal > 0.00
--         AND LEFT(c_phone, 2) IN ('32', '24', '40', '26', '30', '28', '31')
-- ),
-- eligible_customers AS (
--     SELECT
--         LEFT(c.c_phone, 2) AS cntrycode,
--         c.c_acctbal
--     FROM
--         customer c
--         LEFT JOIN orders o ON c.c_custkey = o.o_custkey
--     WHERE
--         LEFT(c.c_phone, 2) IN ('32', '24', '40', '26', '30', '28', '31')
--         AND c.c_acctbal > 0.00
--         AND c.c_acctbal > (SELECT avg_c_acctbal FROM avg_balance)
--         AND o.o_custkey IS NULL
-- )
-- SELECT
--     ec.cntrycode,
--     COUNT(*) AS numcust,
--     SUM(ec.c_acctbal) AS totacctbal
-- FROM
--     eligible_customers ec
-- GROUP BY
--     ec.cntrycode
-- ORDER BY
--     ec.cntrycode;