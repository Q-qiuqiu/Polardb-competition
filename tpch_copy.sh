#!/bin/bash

# default configuration
# user: "postgres"
# database: "postgres"
# host: "localhost"
# primary port: "5432"
pg_user=postgres
pg_database=postgres
pg_host=localhost
pg_port=5432
clean=
tpch_dir=tpch-dbgen
data_dir=/data
usage () {
cat <<EOF

  1) Use default configuration to run tpch_copy
  ./tpch_copy.sh
  2) Use limited configuration to run tpch_copy
  ./tpch_copy.sh --user=postgres --db=postgres --host=localhost --port=5432
  3) Clean the test data. This step will drop the database or tables.
  ./tpch_copy.sh --clean

EOF
  exit 0;
}

for arg do
  val=`echo "$arg" | sed -e 's;^--[^=]*=;;'`

  case "$arg" in
    --user=*)                   pg_user="$val";;
    --db=*)                     pg_database="$val";;
    --host=*)                   pg_host="$val";;
    --port=*)                   pg_port="$val";;
    --clean)                    clean=on ;;
    -h|--help)                  usage ;;
    *)                          echo "wrong options : $arg";
                                exit 1
                                ;;
  esac
done

export PGPORT=$pg_port
export PGHOST=$pg_host
export PGDATABASE=$pg_database
export PGUSER=$pg_user

# clean the tpch test data
if [[ $clean == "on" ]];
then
  #make clean
  if [[ $pg_database == "postgres" ]];
  then
    echo "drop all the tpch tables"
    psql -h /tmp -p 5432 -U postgres -d postgres -c "drop table customer cascade"
    psql -h /tmp -p 5432 -U postgres -d postgres -c "drop table lineitem cascade"
    psql -h /tmp -p 5432 -U postgres -d postgres -c "drop table nation cascade"
    psql -h /tmp -p 5432 -U postgres -d postgres -c "drop table orders cascade"
    psql -h /tmp -p 5432 -U postgres -d postgres -c "drop table part cascade"
    psql -h /tmp -p 5432 -U postgres -d postgres -c "drop table partsupp cascade"
    psql -h /tmp -p 5432 -U postgres -d postgres -c "drop table region cascade"
    psql -h /tmp -p 5432 -U postgres -d postgres -c "drop table supplier cascade"
  else
    echo "drop the tpch database: $PGDATABASE"
    psql -h /tmp -p 5432 -U postgres -d postgres -c "drop database $PGDATABASE"
  fi
  exit;
fi

###################### PHASE 1: create table ######################
if [[ $PGDATABASE != "postgres" ]];
then
  echo "create the tpch database: $PGDATABASE"
  psql -h /tmp -p 5432 -U postgres -d postgres -c "create database $PGDATABASE" -d postgres
fi
psql -h /tmp -p 5432 -U postgres -d postgres -f $tpch_dir/dss.ddl
psql -h /tmp -p 5432 -U postgres -d postgres -c "update pg_class set relpersistence ='u' where relnamespace='public'::regnamespace;"  # 修改 pg_class 中的表持久性为 unlogged

# 设置优化的内存参数
# yzx修改
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET polar_enable_shm_aset = on;" #开启全局共享内存
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET polar_ss_dispatcher_count = 8;" #Dispatcher 进程的最大个数为8
psql -h /tmp -p 5432 -U postgres -d postgres -c "select 'alter table '||tablename||' set (parallel_workers=8);' from pg_tables where schemaname='public';"
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET session_replication_role = 'replica';" #禁用外键约束
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET autovacuum = 'off';" # 关闭 autovacuum  --zgj

psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET maintenance_work_mem = '4GB';" # 提升内存配置以优化索引创建
psql -h /tmp -p 5432 -U postgres -d postgres -c "SELECT pg_reload_conf();"   #使数据生效

# ###################### PHASE 2: load data ######################
split -n l/50 "$data_dir/lineitem.tbl" "$data_dir/lineitem_split_" & # Split the 'lineitem' table into 50 parts
split -n l/15 "$data_dir/orders.tbl" "$data_dir/orders_split_" & # Split the 'orders' table into 15 parts
wait
echo "================data split finished==================="
# 并行运行函数
run_parallel() {
  local max_jobs=$1
  shift
  local commands=("$@")
  local job_count=0

  for cmd in "${commands[@]}"; do
    eval "$cmd" &
    ((job_count++))

    if (( job_count >= max_jobs )); then
      wait -n
      ((job_count--))
    fi
  done

  wait
}

# 准备 COPY 和索引创建命令列表
copy_and_index_commands=()

# 'lineitem' 表的数据导入和索引创建
for i in {a..d}; do
  for j in {a..z}; do
    part="lineitem_split_${i}${j}"
    if [[ -f "$data_dir/$part" ]]; then
      copy_and_index_commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"\\COPY lineitem FROM '$data_dir/$part' WITH (FORMAT csv, DELIMITER '|');\"")
    fi
  done
done

# 'orders' 表的数据导入
for i in {a..b}; do
  for j in {a..z}; do
    part="orders_split_${i}${j}"
    if [[ -f "$data_dir/$part" ]]; then
      copy_and_index_commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"\\COPY orders FROM '$data_dir/$part' WITH (FORMAT csv, DELIMITER '|');\"")
    fi
  done
done

# 其他表
copy_and_index_commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"\\COPY partsupp FROM '$data_dir/partsupp.tbl' WITH (FORMAT csv, DELIMITER '|');\"")
copy_and_index_commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"\\COPY part FROM '$data_dir/part.tbl' WITH (FORMAT csv, DELIMITER '|');\"")
copy_and_index_commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"\\COPY customer FROM '$data_dir/customer.tbl' WITH (FORMAT csv, DELIMITER '|');\"")
copy_and_index_commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"\\COPY supplier FROM '$data_dir/supplier.tbl' WITH (FORMAT csv, DELIMITER '|');\"")
copy_and_index_commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"\\COPY nation FROM '$data_dir/nation.tbl' WITH (FORMAT csv, DELIMITER '|');\"")
copy_and_index_commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"\\COPY region FROM '$data_dir/region.tbl' WITH (FORMAT csv, DELIMITER '|');\"")

# 并行运行导入和索引创建
run_parallel 8 "${copy_and_index_commands[@]}"

wait 
echo "========================data load finished========================"
####################### PHASE 3: add primary and foreign key ######################

# 读取index.txt中的命令，并使用&符号使其在后台执行
while IFS= read -r cmd; do
    # 当已经有N个命令在执行时，等待直到其中一个执行完毕
    # jobs -p -r
    while (( $(jobs -p -r | wc -l) >= 3 )); do
        sleep 0.1
    done
    {
  psql -h /tmp -p 5432 -U postgres -d postgres -c "${cmd}"
    } &
done < index.txt
#run_parallel 4 "${index_commands[@]}"

# 清理临时文件
rm -f "$data_dir/lineitem_split_"*
rm -f "$data_dir/orders_split_"*
# 完成通知
echo "======================index build finished======================="


# 准备主键和外键创建命令列表
commands=()

# Command 1: ORDERS 主键和 LINEITEM 的外键
commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE ORDERS ADD PRIMARY KEY (O_ORDERKEY);\" && \
psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE LINEITEM ADD FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS (O_ORDERKEY) NOT VALID;\"")
# Command 2: PARTSUPP 主键和 LINEITEM 的外键
commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE PARTSUPP ADD PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY);\" && \
psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE LINEITEM ADD FOREIGN KEY (L_PARTKEY, L_SUPPKEY) REFERENCES PARTSUPP (PS_PARTKEY, PS_SUPPKEY) NOT VALID;\"")
# Command 3: LINEITEM 主键
commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE LINEITEM ADD PRIMARY KEY (L_ORDERKEY, L_LINENUMBER);\"")
# Command 4: CUSTOMER 主键和 ORDERS 的外键
commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE CUSTOMER ADD PRIMARY KEY (C_CUSTKEY);\" && \
psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE ORDERS ADD FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER (C_CUSTKEY) NOT VALID;\"")
# Command 5: PART 主键和 PARTSUPP 的外键
commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE PART ADD PRIMARY KEY (P_PARTKEY);\" && \
psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE PARTSUPP ADD FOREIGN KEY (PS_PARTKEY) REFERENCES PART (P_PARTKEY) NOT VALID;\"")
# Command 6: SUPPLIER 主键和 PARTSUPP 的外键
commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE SUPPLIER ADD PRIMARY KEY (S_SUPPKEY);\" && \
psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE PARTSUPP ADD FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER (S_SUPPKEY) NOT VALID;\"")
# Command 7: NATION 主键及 SUPPLIER 和 CUSTOMER 的外键
commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE NATION ADD PRIMARY KEY (N_NATIONKEY);\" && \
psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE SUPPLIER ADD FOREIGN KEY (S_NATIONKEY) REFERENCES NATION (N_NATIONKEY) NOT VALID;\" && \
psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE CUSTOMER ADD FOREIGN KEY (C_NATIONKEY) REFERENCES NATION (N_NATIONKEY) NOT VALID;\"")
# Command 8: REGION 主键和 NATION 的外键
commands+=("psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE REGION ADD PRIMARY KEY (R_REGIONKEY);\" && \
psql -h /tmp -p 5432 -U postgres -d postgres -c \"ALTER TABLE NATION ADD FOREIGN KEY (N_REGIONKEY) REFERENCES REGION (R_REGIONKEY) NOT VALID;\"")


# 并行运行所有主键和外键创建任务
run_parallel 8 "${commands[@]}"
wait 
echo "===========primary key and foreign key create finished==========="

psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER TABLE nation SET (px_workers = 100);"
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER TABLE region SET (px_workers = 100);"
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER TABLE supplier SET (px_workers = 100);"
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER TABLE part SET (px_workers = 100);"
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER TABLE partsupp SET (px_workers = 100);"
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER TABLE customer SET (px_workers = 100);"
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER TABLE orders SET (px_workers = 100);"
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER TABLE lineitem SET (px_workers = 100);"
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET polar_enable_px = ON;"
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET polar_px_dop_per_node = 8;"
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET polar_px_optimizer_enable_hashagg = 0;" #防止内存用尽
psql -h /tmp -p 5432 -U postgres -d postgres -c "update pg_class set relpersistence ='p' where relnamespace='public'::regnamespace;" # 恢复表的持久性设置

psql -h /tmp -p 5432 -U postgres -d postgres -c "SELECT pg_reload_conf();"  
# 约束生效、把unlogged table改为logged table, 生成表统计信息和vm文件.    
psql -h /tmp -p 5432 -U postgres -d postgres -c "update pg_constraint set convalidated=true where convalidated<>true;"  
psql -h /tmp -p 5432 -U postgres -d postgres -c "vacuum analyze;"  
wait
pg_ctl restart -m fast -D ~/tmp_master_dir_polardb_pg_1100_bld 