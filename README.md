# 数据库比赛（天池杯）

## 启动步骤
polardb部署、docker部署、TPC-H工具参考：https://apsaradb.github.io/polardb-pg-docs-v11/zh/development/dev-on-docker.html#%E5%88%9B%E5%BB%BA%E5%B9%B6%E8%BF%90%E8%A1%8C%E5%AE%B9%E5%99%A8

测试平台：https://tianchi.aliyun.com/competition/entrance/532261/score

拉取镜像
```shell
sudo docker pull polardb/polardb_pg_local_instance:11
```

创建容器
```shell
//以当前目录下的所有文件挂载到 /home/postgres/polardb_pg下，不要修改这个路径，要进入PolarDB-for-PostgreSQL目录下执行该命令
sudo docker run -d -it \
    -v $PWD:/home/postgres/polardb_pg \
    --shm-size=512m --cap-add=SYS_PTRACE --privileged=true \
    --name polardb_yzx \
    polardb/polardb_pg_local_instance:11 \
    bash

进入容器：sudo docker exec -it 容器id /bin/bash
```

编译系统文件
```shell
# 获取权限并编译部署
cd polardb_pg
sudo chmod -R a+wr ./
sudo chown -R postgres:postgres ./
./polardb_build.sh
```

使用tpch-dbgen生成数据，并放入data目录下,添加权限
``` 
cd tpch-dbgen/
sudo ./build.sh
# 可以使用 sudo ./build.sh --scale=0.1 来使得生成的测试数据较小
sudo mkdir /data
sudo mv *.tbl /data/
sudo chmod 777 /data/*.tbl && sudo chmod 777 /data
```

若第一次测试无需删除，后续测试都要先删除已建立的表
```
./tpch_copy --clean
```

测试时长
```shell
time ./tpch_copy.sh
```

## 源文件

<img src="D:\software\Typora\project\image\数据库比赛（天池杯）\57ae7ee4f8eb08f7a7dab60cd25f771.png" alt="57ae7ee4f8eb08f7a7dab60cd25f771" style="zoom: 67%;" />

## 导入修改参考

导入数据加速:

- 修改build.sh. 按表并行执行COPY导入任务, 按表的大小从大到小启动COPY导入任务, 逐一启动任务并始终保持N个并行的活跃任务.
- 开启PolarDB预分配功能(使用共享存储时效果比较明显, 预分配可以减少IO次数, 降低云盘IO延迟带来的性能损耗)
  - 默认值可以修改`src/backend/utils/misc/guc.c`实现, 仔细查阅该文件了解更多PolarDB定制参数
- 开始导入数据前使用unlogged table, 在索引创建完成后再改成logged table. 提示:  通过修改pg_class.relpersistence可以实现(u=unlogged,  p=persistence).分区表,RAM,无日志,预分配并行有惊喜
- 参数优化:
  - 导入前关闭autovacuum, 可以降低autoanalyze带来的影响.
  - 加大maintenance_work_mem参数值, 可以提高创建索引的速度.
  - 加大shared_buffers可以提高导入速度
  - 加大checkpoint窗口可以降低检查点对IO的影响
- 使用unix socket代替tcp连接可以提高导入速度

## 导入修改内容

### tpch_copy.sh

```sh
# 设置优化的内存参数
psql -c "ALTER SYSTEM SET polar_bulk_read_size = '128kB';"
psql -c "ALTER SYSTEM SET polar_bulk_extend_size = '4MB';"  #不建议修改
psql -c "ALTER SYSTEM SET polar_index_create_bulk_extend_size = 512;"  #不建议修改
psql -c "ALTER SYSTEM SET maintenance_work_mem = '1GB';"
psql -c "ALTER SYSTEM SET work_mem = '128MB';"
psql -c "ALTER SYSTEM SET shared_buffers = '6GB';"
psql -c "ALTER SYSTEM SET synchronous_commit = 'off';"
psql -c "ALTER SYSTEM SET fsync = 'off';"
psql -c "ALTER SYSTEM SET max_connections = '300';"
psql -c "ALTER SYSTEM SET max_wal_senders = '0';"
psql -c "ALTER SYSTEM SET hot_standby = 'off';"
psql -c "ALTER SYSTEM SET archive_mode = 'off';"
psql -c "ALTER SYSTEM SET wal_log_hints = 'off';"
psql -c "ALTER SYSTEM SET wal_level = 'minimal';"
psql -c "ALTER SYSTEM SET max_replication_slots = 0;"
psql -c "ALTER SYSTEM SET wal_buffers='64MB';"
# 加大checkpoint --zgj
psql -c "ALTER SYSTEM SET checkpoint_timeout = '30min';"
psql -c "ALTER SYSTEM SET max_wal_size = '4GB';"
psql -c "ALTER SYSTEM SET min_wal_size = '80MB';"
psql -c "ALTER SYSTEM SET checkpoint_completion_target = '0.9';"
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET effective_cache_size = '16GB';"
psql -c "ALTER SYSTEM SET polar_wal_pipeline_mode = 1;"
# yzx修改
psql -c "ALTER SYSTEM SET polar_enable_shm_aset = on;" #开启全局共享内存
psql -c "ALTER SYSTEM SET polar_ss_dispatcher_count = 8;" #Dispatcher 进程的最大个数为8

#禁用外键约束
psql -c "ALTER SYSTEM SET session_replication_role = 'replica';"
# 关闭 autovacuum  --zgj
psql -c "ALTER SYSTEM SET autovacuum = 'off';"

psql -h /tmp -p 5432 -U postgres -d postgres -f $tpch_dir/dss.ddl

# 修改 pg_class 中的表持久性为 unlogged
psql -c "update pg_class set relpersistence ='u' where relnamespace='public'::regnamespace;"  
#使数据生效
psql -c "SELECT pg_reload_conf();"  


# ###################### PHASE 2: load data ######################
# Split the 'lineitem' table into 20 parts
split -n l/50 "$data_dir/lineitem.tbl" "$data_dir/lineitem_split_"

# Split the 'orders' table into 10 parts
split -n l/15 "$data_dir/orders.tbl" "$data_dir/orders_split_"

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
      copy_and_index_commands+=("psql -c \"\\COPY lineitem FROM '$data_dir/$part' WITH (FORMAT csv, DELIMITER '|');\" && \
psql -c \"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_lineitem_l_orderkey ON lineitem (l_orderkey);\"")
    fi
  done
done

# 'orders' 表的数据导入和索引创建
for i in {a..b}; do
  for j in {a..z}; do
    part="orders_split_${i}${j}"
    if [[ -f "$data_dir/$part" ]]; then
      copy_and_index_commands+=("psql -c \"\\COPY orders FROM '$data_dir/$part' WITH (FORMAT csv, DELIMITER '|');\" && \
psql -c \"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_o_orderkey ON orders (o_orderkey);\"")
    fi
  done
done

# 'partsupp' 和其他表
copy_and_index_commands+=("psql -c \"\\COPY partsupp FROM '$data_dir/partsupp.tbl' WITH (FORMAT csv, DELIMITER '|');\" && \
psql -c \"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_partsupp_ps_partkey ON partsupp (ps_partkey, ps_suppkey);\"")

copy_and_index_commands+=("psql -c \"\\COPY part FROM '$data_dir/part.tbl' WITH (FORMAT csv, DELIMITER '|');\" && \
psql -c \"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_part_p_partkey ON part (p_partkey);\"")

copy_and_index_commands+=("psql -c \"\\COPY customer FROM '$data_dir/customer.tbl' WITH (FORMAT csv, DELIMITER '|');\" && \
psql -c \"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customer_c_custkey ON customer (c_custkey);\"")

copy_and_index_commands+=("psql -c \"\\COPY supplier FROM '$data_dir/supplier.tbl' WITH (FORMAT csv, DELIMITER '|');\" && \
psql -c \"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_supplier_s_suppkey ON supplier (s_suppkey);\"")

copy_and_index_commands+=("psql -c \"\\COPY nation FROM '$data_dir/nation.tbl' WITH (FORMAT csv, DELIMITER '|');\" && \
psql -c \"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_nation_n_nationkey ON nation (n_nationkey);\"")

copy_and_index_commands+=("psql -c \"\\COPY region FROM '$data_dir/region.tbl' WITH (FORMAT csv, DELIMITER '|');\" && \
psql -c \"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_region_r_regionkey ON region (r_regionkey);\"")

# 并行运行导入和索引创建
run_parallel 8 "${copy_and_index_commands[@]}"

echo "数据导入和索引创建完成。"

# ###################### PHASE 3: add primary and foreign key ######################

# 提升内存配置以优化索引创建
psql -c "ALTER SYSTEM SET maintenance_work_mem = '4GB';"
psql -c "SELECT pg_reload_conf();"

# 准备主键和外键创建命令列表
commands=()

# Command 1: ORDERS 主键和 LINEITEM 的外键
commands+=("psql -c \"ALTER TABLE ORDERS ADD PRIMARY KEY (O_ORDERKEY) USING INDEX idx_orders_o_orderkey;\" && \
psql -c \"ALTER TABLE LINEITEM ADD FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS (O_ORDERKEY) NOT VALID;\"")

# Command 2: PARTSUPP 主键和 LINEITEM 的外键
commands+=("psql -c \"ALTER TABLE PARTSUPP ADD PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY) USING INDEX idx_partsupp_ps_partkey;\" && \
psql -c \"ALTER TABLE LINEITEM ADD FOREIGN KEY (L_PARTKEY, L_SUPPKEY) REFERENCES PARTSUPP (PS_PARTKEY, PS_SUPPKEY) NOT VALID;\"")

# Command 3: LINEITEM 主键
commands+=("psql -c \"ALTER TABLE LINEITEM ADD PRIMARY KEY (L_ORDERKEY, L_LINENUMBER) USING INDEX idx_lineitem_l_orderkey;\"")

# Command 4: CUSTOMER 主键和 ORDERS 的外键
commands+=("psql -c \"ALTER TABLE CUSTOMER ADD PRIMARY KEY (C_CUSTKEY) USING INDEX idx_customer_c_custkey;\" && \
psql -c \"ALTER TABLE ORDERS ADD FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER (C_CUSTKEY) NOT VALID;\"")

# Command 5: PART 主键和 PARTSUPP 的外键
commands+=("psql -c \"ALTER TABLE PART ADD PRIMARY KEY (P_PARTKEY) USING INDEX idx_part_p_partkey;\" && \
psql -c \"ALTER TABLE PARTSUPP ADD FOREIGN KEY (PS_PARTKEY) REFERENCES PART (P_PARTKEY) NOT VALID;\"")

# Command 6: SUPPLIER 主键和 PARTSUPP 的外键
commands+=("psql -c \"ALTER TABLE SUPPLIER ADD PRIMARY KEY (S_SUPPKEY) USING INDEX idx_supplier_s_suppkey;\" && \
psql -c \"ALTER TABLE PARTSUPP ADD FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER (S_SUPPKEY) NOT VALID;\"")

# Command 7: NATION 主键及 SUPPLIER 和 CUSTOMER 的外键
commands+=("psql -c \"ALTER TABLE NATION ADD PRIMARY KEY (N_NATIONKEY) USING INDEX idx_nation_n_nationkey;\" && \
psql -c \"ALTER TABLE SUPPLIER ADD FOREIGN KEY (S_NATIONKEY) REFERENCES NATION (N_NATIONKEY) NOT VALID;\" && \
psql -c \"ALTER TABLE CUSTOMER ADD FOREIGN KEY (C_NATIONKEY) REFERENCES NATION (N_NATIONKEY) NOT VALID;\"")

# Command 8: REGION 主键和 NATION 的外键
commands+=("psql -c \"ALTER TABLE REGION ADD PRIMARY KEY (R_REGIONKEY) USING INDEX idx_region_r_regionkey;\" && \
psql -c \"ALTER TABLE NATION ADD FOREIGN KEY (N_REGIONKEY) REFERENCES REGION (R_REGIONKEY) NOT VALID;\"")

# 并行运行所有主键和外键创建任务
run_parallel 8 "${commands[@]}"

# 清理临时文件
rm -f "$data_dir/lineitem_split_"*
rm -f "$data_dir/orders_split_"*

# 恢复表的持久性设置
psql -c "update pg_class set relpersistence ='p' where relnamespace='public'::regnamespace;"

# 完成通知
echo "主键和外键创建完成，数据加载和索引构建完成。"
```

### guc.c

路径:src/backend/utils/misc/guc.c

```c++
//yzx修改预分配
#define MAX_CONFIG_VARS 100

struct config_bool config_pool[MAX_CONFIG_VARS];
int pool_index = 0; // 当前使用到的预分配池的索引
void DefineCustomBoolVariable(const char *name,
                              const char *short_desc,
                              const char *long_desc,
                              bool *valueAddr,
                              bool bootValue,
                              GucContext context,
                              int flags,
                              GucBoolCheckHook check_hook,
                              GucBoolAssignHook assign_hook,
                              GucShowHook show_hook)
{
    // 检查是否还有可用的预分配空间
    if (pool_index >= MAX_CONFIG_VARS) {
        // 如果超出预分配数量，可以选择扩展池或报错
        elog(ERROR, "Exceeded maximum number of custom bool variables");
        return;
    }

    // 从预分配池中获取下一个可用的 config_bool
    struct config_bool *var = &config_pool[pool_index++];
    
    // 初始化字段
    var->gen.name = name;
    var->gen.short_desc = short_desc;
    var->gen.long_desc = long_desc;
    var->gen.context = context;
    var->gen.flags = flags;
    var->variable = valueAddr;
    var->boot_val = bootValue;
    var->reset_val = bootValue;
    var->check_hook = check_hook;
    var->assign_hook = assign_hook;
    var->show_hook = show_hook;

    // 调试输出
    if (var->assign_hook != NULL && !is_session_dedicated_guc(&var->gen)) {
        ELOG_PSS(DEBUG1, "conf with assign_hook is shared '%s'", var->gen.name);
    }

    // 注册该变量
    define_custom_variable(&var->gen);
}

```

### postgresql.conf.sample

路径:src/backend/utils/misc/postgresql.conf.sample

```shell
shared_buffers = 8GB		
autovacuum = off	
checkpoint_timeout = 1d	
max_wal_size = 128GB
min_wal_size = 64GB
checkpoint_completion_target = 0.9
bgwriter_delay = 10ms			# 10-10000ms between rounds
bgwriter_lru_maxpages = 500		# max buffers written/round, 0 disables
bgwriter_lru_multiplier = 2.0		# 0-10.0 multiplier on buffers scanned/round
bgwriter_flush_after = 512kB 		# measured in pages, 0 disables
force_parallel_mode = on
```

### postgresql.conf.sample.polardb_pg

路径：src/backend/utils/misc/postgresql.conf.sample.polardb_pg

```shell
wal_level=minimal #yzx修改 确保 WAL 级别为 minimal，禁用 WAL 写入
max_wal_senders = 0        # 禁用 WAL 发送器
hot_standby = off           # 禁用热备份
synchronous_commit = off   # 禁用同步提交
max_wal_size=128GB
min_wal_size=64GB
bgwriter_delay=10ms
bgwriter_flush_after=512 #1MB
bgwriter_lru_maxpages=500
max_parallel_workers_per_gather =4
min_parallel_table_scan_size =0
min_parallel_index_scan_size =0
parallel_tuple_cost =0
parallel_setup_cost =0
wal_writer_flush_after=16MB
```

### pg_hba.conf.sample

路径：src/backend/libpq/pg_hba.conf.sample

```c++
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# "local" is for Unix domain socket connections only
local   all             all                                     trust
local   postgres             postgres                                trust
# IPv4 local connections:
host    all             all             127.0.0.1/32            trust
# IPv6 local connections:
host    all             all             ::1/128                 trust
# Allow replication connections from localhost, by a user with the
# replication privilege.
local   replication     all                                     trust
host    replication     all             127.0.0.1/32            trust
host    replication     all             ::1/128                 trust
```

### polardb_build.sh

```c++
 echo "polar_enable_shared_storage_mode = on
        polar_hostid = 1
        max_connections = 400
        polar_wal_pipeline_enable = true
        polar_create_table_with_full_replica_identity = off
        logging_collector = on
        log_directory = 'pg_log'
        unix_socket_directories='/tmp'
        shared_buffers = '6GB'
        synchronous_commit = on
        full_page_writes = off
        #random_page_cost = 1.1
        autovacuum_naptime = 100min
        max_worker_processes = 128
        polar_use_statistical_relpages = off
        polar_enable_persisted_buffer_pool = off
        polar_nblocks_cache_mode = 'all'
        polar_enable_replica_use_smgr_cache = on
        polar_enable_standby_use_smgr_cache = on" >> $pg_bld_master_dir/postgresql.conf
     
gcc_opt_level_flag="-pipe -Wall -grecord-gcc-switches -march=native -mtune=native -fno-omit-frame-pointer -I/usr/include/et"
     
su_eval "$pg_bld_basedir/bin/initdb -U $pg_db_user -D $pg_bld_master_dir --no-locale --encoding=SQL_ASCII $tde_initdb_args"
```

### heapam.h

路径：src/include/access/heapam.h

```c++
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

// 定义线程参数结构
typedef struct
{
    Relation relation;
    HeapTuple *inputTuples;
    HeapTuple *outputTuples;
    int start;
    int end;
    TransactionId xid;
    CommandId cid;
    int options;
} ThreadArgs;
void *prepare_insert_range(void *args);
void parallel_prepare_insert(Relation relation, HeapTuple *inputTuples, HeapTuple *outputTuples,
                             int ntuples, TransactionId xid, CommandId cid, int options, int numThreads);
```

### heapam.c

路径：src/backend/access/heap/heapam.c

```c++
// 单线程处理函数
void *prepare_insert_range(void *args)
{
    ThreadArgs *threadArgs = (ThreadArgs *)args;

    for (int i = threadArgs->start; i < threadArgs->end; i++)
    {
        threadArgs->outputTuples[i] = heap_prepare_insert(
            threadArgs->relation,
            threadArgs->inputTuples[i],
            threadArgs->xid,
            threadArgs->cid,
            threadArgs->options);
    }

    return NULL;
}

// 并行处理函数
void parallel_prepare_insert(Relation relation, HeapTuple *inputTuples, HeapTuple *outputTuples,
                             int ntuples, TransactionId xid, CommandId cid, int options, int numThreads)
{
    pthread_t threads[numThreads];
    ThreadArgs threadArgs[numThreads];
    int batchSize = ntuples / numThreads;

    // 创建线程
    for (int i = 0; i < numThreads; i++)
    {
        int start = i * batchSize;
        int end = (i == numThreads - 1) ? ntuples : start + batchSize;

        threadArgs[i].relation = relation;
        threadArgs[i].inputTuples = inputTuples;
        threadArgs[i].outputTuples = outputTuples;
        threadArgs[i].start = start;
        threadArgs[i].end = end;
        threadArgs[i].xid = xid;
        threadArgs[i].cid = cid;
        threadArgs[i].options = options;

        if (pthread_create(&threads[i], NULL, prepare_insert_range, &threadArgs[i]) != 0)
        {
            perror("Failed to create thread");
            exit(EXIT_FAILURE);
        }
    }
    // 等待线程完成
    for (int i = 0; i < numThreads; i++)
    {
        if (pthread_join(threads[i], NULL) != 0)
        {
            perror("Failed to join thread");
            exit(EXIT_FAILURE);
        }
    }
}
```

## 查询修改参考

- 使用单机并行, 通过修改表的配置和参数可启用强制并行度。
- 开启PolarDB预读功能，使用共享存储时效果比较明显，预读可以减少IO次数，降低云盘IO延迟带来的性能损耗。
  - 说明：默认值可以修改src/backend/utils/misc/guc.c实现, 仔细查阅该文件了解更多PolarDB定制参数。
- 修改配置，例如优化器校准因子相关配置、优化器JOIN方法相关配置、哈希表内存大小相关配置等。
- 使用列存储和JIT，能节约存储空间、加速导入、加速查询，通常可以比行存储性能提升10倍以上。
  - 说明：需要修改内核才能实现优化。
- 参数优化
  - 加大shared_buffers可以提高查询速度。
  - 加大work_mem可以提高查询速度。
- 通过索引可以提升某些SQL的查询性能。
  - 说明：加索引也会导致占用更多的空间以及建索引本身的耗时。
- 对于极限测试，每一条 SQL 都可以单独优化（例如使用不同的参数、JOIN 方法、索引等）。如果需要简单的方式，可以调整 tpch 的测试脚本；如果需要复杂一些的方式，可以修改 hook。而最具实用价值的做法是改进优化器，以实现通用的复杂 SQL 优化。
  - 说明：还有一些情况下，即使使用相同的配置，某些SQL可能仍然会导致内存耗尽并发生OOM。这可能是由于hash table过大造成的，因此可以考虑支持hash table split to disk，多阶段join
  
  开启单机并行参考：https://apsaradb.github.io/polardb-pg-docs-v11/zh/operation/tpch-test.html#%E6%89%A7%E8%A1%8C-epq-%E5%8D%95%E6%9C%BA%E5%B9%B6%E8%A1%8C%E6%89%A7%E8%A1%8C
  
  决赛提交指南参考：https://github.com/digoal/blog/blob/master/202412/20241206_05.md

## 常用命令

**查看相关参数**

关于日志WAL：select name,setting,unit,category,extra_desc  from pg_settings where name like '%wal%';

关于页：select name,setting,unit,category,extra_desc  from pg_settings where name like '%page%';

关于vacuum：select name,setting,unit,category,extra_desc  from pg_settings where name like '%vacuum%';

重启数据库：pg_ctl restart -m fast -D ~/tmp_master_dir_polardb_pg_1100_bld  

打包：zip -r PolarDB-for-PostgreSQL.zip PolarDB-for-PostgreSQL/  

复制：docker cp polardb_pg_devel:/tmp/PolarDB-for-PostgreSQL.zip /home/yzx/  

删除所有索引

```sql
DO $$
DECLARE
    index_name TEXT;
BEGIN
    FOR index_name IN
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname NOT IN (
              SELECT conname
              FROM pg_constraint
              WHERE contype IN ('p', 'f') -- 主键 (p) 和外键 (f)
          )
    LOOP
        EXECUTE 'DROP INDEX IF EXISTS public.' || index_name;
    END LOOP;
END $$;
```

查询索引

```c++
SELECT indexname
FROM pg_indexes
WHERE tablename IN ('lineitem', 'part', 'partsupp', 'customer', 'orders', 'nation', 'region', 'supplier')
  AND schemaname = 'public';  -- 根据需要调整模式名
```

查询计划及耗时：

```shell
echo "`sed 's@^select@explain (analyze,verbose,timing,costs,buffers) select@' 1.sql`" | psql -f -
```

停止所有计划

```c++
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state = 'active' AND pid <> pg_backend_pid();
```

## 查询修改内容

### tpch_copy.sh

```shell
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET polar_enable_shm_aset = on;" #开启全局共享内存
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET polar_ss_dispatcher_count = 8;" #Dispatcher 进程的最大个数为8
psql -h /tmp -p 5432 -U postgres -d postgres -c "select 'alter table '||tablename||' set (parallel_workers=8);' from pg_tables where schemaname='public';"
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET session_replication_role = 'replica';" #禁用外键约束
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET autovacuum = 'off';" # 关闭 autovacuum 
psql -h /tmp -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET maintenance_work_mem = '4GB';" # 提升内存配置以优化索引创建
psql -c "ALTER TABLE nation SET (px_workers = 100);"
psql -c "ALTER TABLE region SET (px_workers = 100);"
psql -c "ALTER TABLE supplier SET (px_workers = 100);"
psql -c "ALTER TABLE part SET (px_workers = 100);"
psql -c "ALTER TABLE partsupp SET (px_workers = 100);"
psql -c "ALTER TABLE customer SET (px_workers = 100);"
psql -c "ALTER TABLE orders SET (px_workers = 100);"
psql -c "ALTER TABLE lineitem SET (px_workers = 100);"
psql -c "ALTER SYSTEM SET polar_enable_px = ON;"
psql -c "ALTER SYSTEM SET polar_px_dop_per_node = 8;"
psql -c "ALTER SYSTEM SET polar_px_optimizer_enable_hashagg = 0;" #防止内存用尽
psql -c "alter table pa set (parallel_workers =8);"
psql -c "SELECT pg_reload_conf();"  
```

### dss.ri

创建索引

```c++
-- 1
create index i1_1 on lineitem (l_returnflag,l_linestatus) include (l_quantity,l_extendedprice,l_discount,l_tax) where l_shipdate <= date '1998-08-05';

-- 2
CREATE INDEX i2_1 ON partsupp (ps_partkey, ps_suppkey) INCLUDE (ps_supplycost); --8，9也用
CREATE INDEX i2_2 ON part (p_partkey) INCLUDE (p_size, p_type) WHERE p_size = 28 AND p_type LIKE '%COPPER'; 

-- 3 
create index i3_1 on customer (c_custkey) where c_mktsegment = 'BUILDING';
create index i3_2 on orders (o_custkey) include (o_orderdate,o_shippriority) where o_orderdate < date '1995-03-07';
create index i3_3 on lineitem (l_orderkey) include (l_extendedprice,l_discount) where l_shipdate > date '1995-03-07';

-- 4 
create index i4_1 on lineitem (l_orderkey) where l_commitdate < l_receiptdate; -- 12也会用到
create index i4_2 on orders (o_orderpriority) where o_orderdate >= date '1994-02-01' and o_orderdate < date '1994-05-01';

-- 5  
create index i5_1 on orders (o_custkey) include (o_orderkey) where o_orderdate >= date '1993-01-01' and o_orderdate < date '1994-01-01';  
CREATE INDEX i5_2 ON supplier (s_suppkey, s_nationkey);

-- 6
CREATE INDEX i6_1 ON lineitem (l_discount) INCLUDE (l_extendedprice, l_quantity) WHERE l_shipdate >= DATE '1993-01-01' AND l_shipdate < DATE '1994-01-01';

-- 7
CREATE INDEX i7_1 ON lineitem (l_suppkey) INCLUDE (l_extendedprice, l_shipdate, l_quantity) WHERE l_shipdate >= DATE '1995-01-01' AND l_shipdate < DATE '1996-12-31'; --12-20 7没有用到，真神奇
create index i7_2 on nation (n_nationkey); -- 8,9也用
-- 8  
CREATE INDEX i8_1 ON orders (o_orderdate, o_orderkey, o_custkey) WHERE o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31';
create index i8_2 on part (p_partkey) where p_type = 'ECONOMY BRUSHED BRASS';
CREATE INDEX i8_4 ON customer (c_custkey, c_nationkey);

-- 9
CREATE EXTENSION pg_trgm;
CREATE INDEX i9_2 ON part USING gin(p_name gin_trgm_ops);

-- 10 
create index i10_1 on lineitem (l_orderkey) include (l_extendedprice,l_discount) where l_returnflag = 'R';
create index i10_2 on orders (o_orderkey) include (o_custkey) where o_orderdate >= date '1993-12-01' and o_orderdate < date '1994-3-01';

-- 12
CREATE INDEX i12_1 ON lineitem (l_commitdate) INCLUDE (l_shipmode,l_shipdate, l_receiptdate,l_orderkey) WHERE l_receiptdate >= DATE '1996-01-01' AND l_receiptdate < DATE '1997-01-01';

-- 14
create index i14_1 on lineitem(l_partkey) include (l_extendedprice,l_discount) where l_shipdate >= date '1996-07-01' and l_shipdate < date '1996-08-01';

-- 15
CREATE INDEX i15_1 ON lineitem (l_suppkey) INCLUDE (l_extendedprice, l_discount) WHERE l_shipdate >= DATE '1996-09-01' AND l_shipdate < DATE '1996-12-01';

-- 17
create index i17_1 on part (p_partkey) where p_brand = 'Brand#35' and p_container = 'JUMBO PKG';
create index i17_2 on lineitem (l_partkey) include (l_quantity,l_extendedprice);

-- 18
create index i18_1 on lineitem (l_orderkey) include (l_quantity);

-- 20
create index i20_1 on lineitem (l_partkey,l_suppkey) where l_shipdate >= date '1997-01-01' and l_shipdate < date '1998-01-01';
create index i20_2 on part (p_partkey) where p_name like 'wheat%';

-- 21
create index i21_1 on lineitem (l_orderkey) include (l_suppkey) where l_receiptdate <> l_commitdate;
create index i21_2 on lineitem (l_orderkey) include (l_suppkey);
create index i21_3 on orders (o_orderkey) where o_orderstatus = 'F';
```

## SQL语句分析

### 1.sql 定价汇总报表查询

```sql
select 
  l_returnflag, //返回标志
  l_linestatus, 
  sum(l_quantity) as sum_qty, //总的数量
  sum(l_extendedprice) as sum_base_price, //聚集函数操作
  sum(
    l_extendedprice * (1 - l_discount)
  ) as sum_disc_price, 
  sum(
    l_extendedprice * (1 - l_discount) * (1 + l_tax)
  ) as sum_charge, 
  avg(l_quantity) as avg_qty, 
  avg(l_extendedprice) as avg_price, 
  avg(l_discount) as avg_disc, 
  count(*) as count_order //每个分组所包含的行数
from 
  lineitem 
where 
  l_shipdate <= date '1998-12-01' - interval '118' day 
group by 
  l_returnflag, 
  l_linestatus 
order by 
  l_returnflag, 
  l_linestatus;
```

它的目的是分析订单中不同退货标记 (`l_returnflag`) 和行状态 (`l_linestatus`) 下的销售数据:

总数量（`sum_qty`）

总销售价格（`sum_base_price`）

计算折扣后销售价格（`sum_disc_price`）

计算税后销售价格（`sum_charge`）

平均数量、价格和折扣（`avg_qty`, `avg_price`, `avg_disc`）

订单数量（`count_order`）。

筛选条件为 `l_shipdate <= '1998-08-05'- interval '118' day `，即筛选出发货日期在1998年8月5日再提前118天前的记录。

创建索引建议：

只针对`WHERE l_shipdate <= date '1998-12-01' - interval '118' day`范围内创建索引，`l_returnflag `和 `l_linestatus`是关键列，`l_quantity, l_extendedprice, l_discount, l_tax` 包含为“覆盖列”，不会作为索引，但是可以从索引中读取

```sql
create index i1 on lineitem (l_returnflag,l_linestatus) include (l_quantity,l_extendedprice,l_discount,l_tax) where l_shipdate <= date '1998-12-01' - interval '118' day;
```

### 2.sql 最低成本供应商查询

```sql
select 
  s_acctbal, 
  s_name, 
  n_name, 
  p_partkey, 
  p_mfgr, 
  s_address, 
  s_phone, 
  s_comment  /*查询供应者的帐户余额、名字、国家、零件的号码、生产者、供应者的地址、电话号码、备注信息 */
from 
  part, 
  supplier, 
  partsupp, 
  nation, 
  region 
where 
  p_partkey = ps_partkey 
  and s_suppkey = ps_suppkey 
  and p_size = 28  //指定大小
  and p_type like '%COPPER'  //指定类型
  and s_nationkey = n_nationkey 
  and n_regionkey = r_regionkey 
  and r_name = 'AMERICA' //指定地区
  and ps_supplycost = ( //子查询
    select 
      min(ps_supplycost) //聚集函数
    from 
      partsupp, 
      supplier, 
      nation, 
      region //与父查询的表有重叠
    where 
      p_partkey = ps_partkey 
      and s_suppkey = ps_suppkey 
      and s_nationkey = n_nationkey 
      and n_regionkey = r_regionkey 
      and r_name = 'AMERICA'
  ) 
order by //排序
  s_acctbal desc, 
  n_name, 
  s_name, 
  p_partkey;

```

从多个表（`part`, `supplier`, `partsupp`, `nation`, `region`）中筛选符合条件的记录：

零件大小 `p_size = 28`。

零件类型包含 "COPPER"（`p_type LIKE '%COPPER'`）。

地区为 "AMERICA"（`r_name = 'AMERICA'`）。

供应成本等于最低值（通过子查询实现）:计算每个 `p_partkey` 在 "AMERICA" 地区供应的最低供应成本 `min(ps_supplycost)`。

创建索引建议：

```sql
-- 针对 p_size 和 p_type 的过滤条件
CREATE INDEX idx_part_size_type ON part (p_size, p_type);
-- 针对 r_name 的等值过滤条件
CREATE INDEX idx_region_name ON region (r_name);
-- 针对 n_regionkey（连接键）
CREATE INDEX idx_nation_regionkey ON nation (n_regionkey);
-- 针对 s_nationkey（连接键）和排序列 s_acctbal
CREATE INDEX idx_supplier_nationkey_acctbal ON supplier (s_nationkey, s_acctbal DESC);
-- 针对主查询和子查询中的 p_partkey、s_suppkey 和 ps_supplycost：
CREATE INDEX idx_partsupp_partkey_supplycost ON partsupp (p_partkey, s_suppkey, ps_supplycost);
-- 子查询中计算 min(ps_supplycost) 的部分索引 
CREATE INDEX idx_partsupp_supplycost_region ON partsupp (p_partkey, ps_supplycost) INCLUDE (s_suppkey);
```

### 3.sql 运输优先级查询

```sql
select
    l_orderkey,
    sum(l_extendedprice*(1-l_discount))as revenue,//潜在的收入，聚集操作
    o_orderdate,
    o_shippriority
from
    customer,
    orders,
    lineitem
where
    c_mktsegment='BUILDING'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate <date '1995-03-07'
    and l_shipdate  >date '1995-03-07'//指定日期段
group by
    l_orderkey,//订单标识
    o_orderdate,//订单日期
    o_shippriority//运输优先级
order by
    revenue desc,//降序排序，把潜在最大收入列在前面
    o_orderdate
LIMIT 10;
```

该查询从三个表 `customer`、`orders` 和 `lineitem` 中选择订单相关的信息，计算每个订单的收入，并按收入和订单日期排序：

`c_mktsegment = 'BUILDING'`: 限制客户的市场段为 "BUILDING"。

`c_custkey = o_custkey`: 将客户表和订单表通过客户键关联。

`l_orderkey = o_orderkey`: 将订单表和订单明细表通过订单键关联。

`o_orderdate < '1995-03-07'`: 限制订单日期在 1995-03-07 之前。

`l_shipdate > '1995-03-07'`: 限制订单的发货日期在 1995-03-07 之后。

创建索引建议：

```sql
-- 查询条件使用了 c_mktsegment，且需要快速查找主键 c_custkey。
create index i2 on customer (c_custkey) where c_mktsegment = 'BUILDING';
-- 查询条件使用了 o_orderdate 和关联键 o_custkey。
create index i3 on orders (o_custkey) include (o_orderdate,o_shippriority) where o_orderdate < date '1995-03-07';
-- 查询条件使用了 l_orderkey 和 l_shipdate，且涉及计算列。
create index i4 on lineitem (l_orderkey) include (l_extendedprice,l_discount) where l_shipdate > date '1995-03-07';
```

### 4.sql 订单优先级查询

```sql
select
    o_orderpriority,//订单优先级
    count(*)as order_count//订单优先级计数
from
    orders
where
    o_orderdate>=date'1994-02-01'
    and o_orderdate<date'1994-02-01'+ interval '3' month//指定订单的时间段
    and exists(//子查询
        select*
        from
            lineitem
        where
    l_orderkey =o_orderkey
    and l_commitdate<l_receiptdate
    )
group by //按订单优先级分组
    o_orderpriority
order by//按订单优先级排序
    o_orderpriority;
```

统计符合条件的订单优先级及其订单数量，并按照优先级排序：

`o_orderdate` 在 1994-02-01 至 1994-05-01 之间。

存在关联的 `lineitem` 记录，且 `l_commitdate < l_receiptdate`。

索引添加建议：

```sql
CREATE INDEX i_orders ON orders (o_orderkey, o_orderpriority) WHERE o_orderdate >= DATE '1994-02-01' AND o_orderdate < DATE '1994-02-01' + INTERVAL '3' MONTH;
CREATE INDEX i_lineitem ON lineitem (l_orderkey) WHERE l_commitdate < l_receiptdate;
```

### 5.sql 本地供应商收入量查询

```sql
select
    n_name,
    sum(l_extendedprice *(1-l_discount))as revenue//聚集操作
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
    c_custkey =o_custkey
    and l_orderkey=o_orderkey
    and l_suppkey=s_suppkey
    and c_nationkey=s_nationkey
    and s_nationkey =n_nationkey
    and n_regionkey=r_regionkey
    and r_name ='ASIA'//指定地区
    and o_orderdate>=date'1993-01-01'
    and o_orderdate<date'1993-01-01'+ interval '1' year//DATE
group by//按名字分组
    n_name
order by
    revenue desc//按收入降序排序，注意分组和排序子句不同
LIMIT 10;
```

此查询聚合了在 **ASIA** 区域内的客户、订单、商品和供应商数据，计算每个国家的销售收入，并按收入降序排序：

`r_name = 'ASIA'`: 筛选区域为 "ASIA"。

`o_orderdate` 范围：1993 年全年

索引添加建议：

```sql
CREATE INDEX i_region ON region (r_regionkey) WHERE r_name = 'ASIA';
CREATE INDEX i_orders ON orders (o_orderkey, o_custkey) INCLUDE (o_orderdate) WHERE o_orderdate >= DATE '1993-01-01' AND o_orderdate < DATE '1993-01-01' + INTERVAL '1' YEAR;
CREATE INDEX i_lineitem ON lineitem (l_orderkey, l_suppkey) INCLUDE (l_extendedprice, l_discount);
CREATE INDEX i_supplier ON supplier (s_suppkey, s_nationkey);
CREATE INDEX i_nation ON nation (n_nationkey, n_regionkey);
```

### 6.sql 预测收入变化查询

```sql
select
    sum(l_extendedprice *l_discount)as revenue//潜在的收入增加量
from
    lineitem
where
    l_shipdate>= date'1993-01-01'
    and l_shipdate<date'1993-01-01'+interval '1'year//DATE
    and l_discount between 0.07-0.01 and 0.07 + 0.01
    and l_quantity< 25
LIMIT 10;
```

聚合了 `lineitem` 表的数据，计算在特定时间范围内、满足折扣和数量条件的订单收入：

**日期范围过滤**：

- `l_shipdate >= DATE '1993-01-01'` 和 `l_shipdate < DATE '1993-01-01' + INTERVAL '1' YEAR`。

**折扣范围过滤**：

- `l_discount BETWEEN 0.07 - 0.01 AND 0.07 + 0.01`。

**数量过滤**：

- `l_quantity < 25`。

添加索引建议：

```sql
CREATE INDEX i_lineitem ON lineitem (l_shipdate, l_discount, l_quantity) INCLUDE (l_extendedprice);
```

### 7.sql 批量出货查询

```sql
select
    supp_nation,//供货商国家
    cust_nation,//顾客国家
    l_year,//年度
    sum(volume)as revenue//年度的货运收入
from
    (//子查询
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate)as l_year,
            l_extendedprice*(1-l_discount)as volume
        from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        where
            s_suppkey = l_suppkey
            and o_orderkey=l_orderkey
            and c_custkey=o_custkey
            and s_nationkey =n1.n_nationkey
            and c_nationkey =n2.n_nationkey
            and(// NATION2和NATION1的值不同，表示查询的是跨国的货运情况
                (n1.n_name ='PERU' and n2.n_name ='VIETNAM')
                or(n1.n_name ='VIETNAM'and n2.n_name='PERU')
            )
            and l_shipdate between date '1995-01-01'and date '1996-12-31'
        )as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year
LIMIT 10;
```

从多表联合查询中获取 `PERU` 和 `VIETNAM` 之间的供应商和客户的交易记录，按年份分组计算收入：

`n1.n_name = 'PERU' AND n2.n_name = 'VIETNAM'` 或反向条件。

`l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'`。

按 `supp_nation`（供应商国籍）、`cust_nation`（客户国籍）和 `l_year`（年份）分组，计算收入总和。

索引添加推荐：

```sql
CREATE INDEX i_lineitem_optimized ON lineitem (l_shipdate, l_suppkey, l_orderkey) INCLUDE (l_extendedprice, l_discount);
CREATE INDEX i_orders_time ON orders (o_orderkey, o_custkey);
CREATE INDEX i_supplier ON supplier (s_suppkey, s_nationkey);
CREATE INDEX i_customer_nation ON customer (c_custkey, c_nationkey);
CREATE INDEX i_nation_name ON nation (n_name, n_nationkey);
```

### 8.sql 全国市场份额查询

```sql
select
    o_year,
    sum(case
        when nation ='VIETNAM' then volume
        else 0
    end)/sum(volume)as mkt_share//市场份额：特定种类的产品收入的百分比；聚集操作
from
    (
        select
            extract(year from o_orderdate)as o_year,//分解出年份
            l_extendedprice*(1-l_discount)as volume, //特定种类的产品收入
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
            p_partkey =l_partkey
            and s_suppkey=l_suppkey
            and l_orderkey=o_orderkey
            and o_custkey=c_custkey
            and c_nationkey =n1.n_nationkey
            and n1.n_regionkey=r_regionkey
            and r_name ='ASIA'//指定地区
            and s_nationkey =n2.n_nationkey
            and o_orderdate between date'1995-01-01'
            and date '1996-12-31'
            and p_type='ECONOMY BRUSHED BRASS'//指定零件类型
    )as all_nations
group by
    o_year
order by
    o_year
LIMIT 10;
```

计算特定商品类型（`p_type = 'ECONOMY BRUSHED BRASS'`）在特定地区（`r_name = 'ASIA'`）的市场份额随年份的变化趋势。

**时间范围过滤**：`o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'`。

**具体类型过滤**：`p_type = 'ECONOMY BRUSHED BRASS'`。

**连接优化**：支持多表连接的字段。

**计算优化**：收入计算涉及的字段，如 `l_extendedprice` 和 `l_discount`。

索引推荐：

```sql
create index i8 on orders (o_orderkey) where o_orderdate between date '1995-01-01' and date '1996-12-31';
create index i9 on part (p_partkey) where p_type = 'ECONOMY BRUSHED BRASS';
```

### 9.sql 产品类型利润度量查询

```sql
select
    nation,
    o_year,
    sum(amount)as sum_profit//每个国家每一年所有被定购的零件在一年中的总利润
from
    (
        select
            n_name as nation,//国家
            extract(year from o_orderdate)as o_year,//取出年份
            l_extendedprice*(1-l_discount)-ps_supplycost*l_quantity as amount//利润
        from
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        where
            s_suppkey =l_suppkey
            and ps_suppkey=l_suppkey
            and ps_partkey =l_partkey
            and p_partkey=l_partkey
            and o_orderkey=l_orderkey
            and s_nationkey=n_nationkey
            and p_name like'%sandy%'//LIKE操作，查询优化器可能进行优化
    )as profit
group by
    nation,
    o_year
order by//按国家和年份排序，年份大者靠前
    nation,
    o_year desc
LIMIT 10;
```

计算不同国家在不同年份的利润总额，从 `part`, `supplier`, `lineitem`, `partsupp`, `orders`, 和 `nation` 表中获取数据，并根据 `p_name` 包含“sandy”的条件过滤数据。查询中计算了每个订单的利润（`l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity`）

添加索引推荐：

```sql
create index idx_orders_o_orderdate on orders (o_orderdate);
create index idx_lineitem_l_suppkey_l_partkey on lineitem (l_suppkey, l_partkey);
create index idx_partsupp_ps_suppkey_ps_partkey on partsupp (ps_suppkey, ps_partkey);
create index idx_supplier_s_nationkey on supplier (s_nationkey);
create index idx_part_p_name on part (p_name) where p_name like '%sandy%';
```

### 10.sql 退货报告查询

```sql
select
    c_custkey, //客户信息
    c_name,
    sum(l_extendedprice*(1-l_discount))as revenue,//收入损失
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment//国家、地址、电话、意见信息等
from
    customer,
    orders,
    lineitem,
    nation
where
    c_custkey =o_custkey
    and l_orderkey =o_orderkey
    and o_orderdate>=date'1993-12-01'and o_orderdate<date'1993-12-01'+ interval'3' month
    and l_returnflag='R'//货物被回退
    and c_nationkey =n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
LIMIT 20;
```

用于获取客户在特定时间段内的销售收入数据，并按客户的收入排序。具体来说，它计算每个客户的收入（`revenue`），包括客户信息（如姓名、账户余额、地址、电话和评论），以及客户所在国家的名称。查询筛选了特定时间范围内的订单，并仅包含返回标志为“R”的订单行。

索引添加建议：

```sql
create index idx_orders_date_returnflag on orders (o_orderdate, o_custkey) where o_orderdate >= date '1993-12-01' and o_orderdate < date '1993-12-01' + interval '3' month;
create index idx_lineitem_returnflag on lineitem (l_orderkey, l_returnflag) where l_returnflag = 'R';
create index idx_customer_nation on customer (c_custkey, c_nationkey);
```

### 11.sql 库存价值查询

```sql
select
    ps_partkey,
    sum(ps_supplycost*ps_availqty)as value //聚集操作，商品的总价值
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey=s_suppkey
    and s_nationkey =n_nationkey
    and n_name ='INDONESIA'
group by
    ps_partkey having//带有HAVING子句的分组操作
        sum(ps_supplycost*ps_availqty)>(//HAVING子句中包括有子查询
            select
                sum(ps_supplycost*ps_availqty)*0.0001000000//子查询中存在聚集操作
            from
            partsupp,
            supplier,
            nation//与父查询的表连接一致
            where//与父查询的WHEWR条件一致
            ps_suppkey=s_suppkey
            and s_nationkey=n_nationkey
            and n_name ='INDONESIA'
        )
order by //按商品的价值降序排序
    value desc
LIMIT 10;
```

找出在 `INDONESIA` 国家中，按 `ps_partkey` 分组的供应商提供的供应品的总价值（`ps_supplycost * ps_availqty`），并筛选出这些总价值大于整体供应品总价值的0.0001的部分。

索引添加建议：

```sql
create index idx_nation_name on nation (n_name);
create index idx_partsupp_suppkey_availqty on partsupp (ps_suppkey, ps_partkey, ps_supplycost, ps_availqty);
```

### 12.sql 运送方式和订单优先级查询

```sql
select
    l_shipmode,
    sum(//聚集操作
        case when o_orderpriority='1-URGENT'//OR运算，二者满足其一即可，选出URGENT或HIGH的
            or o_orderpriority='2-HIGH'
            then 1
        else 0
    end)as high_line_count,
    sum(case
        when o_orderpriority<>'1-URGENT'
            and o_orderpriority<>'2-HIGH'//AND运算，二者都不满足，非URGENT非HIGH的
            then 1
        else 0
    end)as low_line_count
from
    orders,
    lineitem
where
    o_orderkey =l_orderkey
    and l_shipmode in('MAIL','SHIP')// 指定货运模式的类型
    and l_commitdate< l_receiptdate
    and l_shipdate <l_commitdate
    and l_receiptdate>=date'1996-01-01'
    and l_receiptdate<date'1996-01-01'+ interval'1'
year
group by
    l_shipmode
order by
    l_shipmode
LIMIT 10;
```

统计在特定时间范围内（`l_receiptdate` 在 `1996-01-01` 和 `1996-12-31` 之间）按送货方式 (`l_shipmode`) 进行的订单的数量。查询会分别统计两类订单数量：

- **高优先级订单 (`o_orderpriority` 为 `'1-URGENT'` 或 `'2-HIGH'`)** 的数量。
- **低优先级订单 (`o_orderpriority` 不为 `'1-URGENT'` 或 `'2-HIGH'`)** 的数量。

添加索引建议：

```sql
create index idx_orders_priority_date on orders (o_orderpriority, o_orderkey, o_orderdate);
create index idx_lineitem_shipmode_dates on lineitem (l_shipmode, l_orderkey, l_commitdate, l_receiptdate, l_shipdate);
```

### 13.sql 客户分布查询

```sql
select
    c_count,
    count(*)as custdist //聚集操作，统计每个组的个数
from
    (
    select
        c_custkey,
        count(o_orderkey)
    from
        customer left outer join orders on //子查询中包括左外连接操作
            c_custkey =o_custkey
            and o_comment not like '%express%accounts%'
    group by //子查询中的分组操作
            c_custkey
    )as c_orders(c_custkey,c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc
LIMIT 10;
```

计算每个客户在特定条件下的订单数量，并统计每种订单数量的客户数量：

 `o_comment` 不包含“express accounts”的情况

添加索引建议：

```sql
-- 为 customer 表中的 c_custkey 创建索引
create index idx_customer_custkey on customer (c_custkey);
-- 为 orders 表中的 o_custkey 和 o_comment 创建索引，优化连接和排除特定条件
create index idx_orders_custkey_comment on orders (o_custkey) where o_comment not like '%express%accounts%';
```

### 14.sql 促销效果查询

```sql
select
    100.00* sum(case
        when p_type like 'PROMO%'//促销零件
            then l_extendedprice*(1-l_discount)//某一特定时间的收入
        else 0
    end)/sum(l_extendedprice*(1-l_discount))as promo_revenue
from
    lineitem,
    part
where
    l_partkey =p_partkey
    and l_shipdate >= date'1996-07-01'
    and l_shipdate<date'1996-07-01'+ interval '1' month
LIMIT 10;
```

计算在指定时间范围内，`lineitem` 表中属于促销类型的销售额占总销售额的百分比：

- **`p_type like 'PROMO%'`**：筛选出 `part` 表中 `p_type` 以 "PROMO" 开头的记录，用于计算促销商品的销售额。
- **`l_shipdate >= date'1996-07-01' and l_shipdate < date'1996-07-01' + interval '1' month`**：限定在 1996 年 7 月份的数据。

添加索引建议：

```sql
-- 为 part 表中的 p_partkey 和 p_type 创建索引，优化连接和促销类型的过滤
create index idx_part_p_type on part (p_partkey, p_type);
-- 为 lineitem 表中的 l_partkey 和 l_shipdate 创建索引，优化连接和日期范围过滤
create index idx_lineitem_shipdate_partkey on lineitem (l_partkey, l_shipdate) where l_shipdate >= date'1996-07-01' and l_shipdate < date'1996-07-01' + interval '1' month;

```

### 15.sql 顶级供应商查询

```sql
create view revenue0(supplier_no,total_revenue)as//创建复杂视图（带有分组操作）
    select
        l_suppkey,sum(l_extendedprice*(1-l_discount))//获取供货商为公司带来的总利润
    from
        lineitem
    where
        l_shipdate>= date'1996-09-01'and l_shipdate<date'1996-09-01'+ interval'3'
month
    group by
        l_suppkey;

select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    revenue0//普通表与复杂视图进行连接操作
where
    s_suppkey=supplier_no
    and total_revenue =(//聚集子查询
        select
            max(total_revenue)
        from
        revenue0 //聚集子查询从视图获得数据
    )
order by
    s_suppkey;
drop view revenue0;
```

找到在指定时间范围内（1996 年 9 月 1 日至 12 月 1 日）销售总收入最高的供应商信息：

创建一个视图 `revenue0`，用于存储每个供应商的总销售额。

从 `supplier` 表中获取与 `revenue0` 视图中总销售额最高的供应商相关的信息。

显示这些供应商的 `s_suppkey`、`s_name`、`s_address`、`s_phone` 和 `total_revenue`。

```sql
create index idx_lineitem_suppkey_shipdate on lineitem (l_suppkey, l_shipdate) where l_shipdate >= date'1996-09-01' and l_shipdate < date'1996-09-01' + interval '3' month;
create index idx_supplier_suppkey on supplier (s_suppkey);
```

### 16.sql 零部件/供货商关系查询

```sql
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey)as supplier_cnt//聚集、去重操作
from
    partsupp,
    part
where
    p_partkey =ps_partkey
    and p_brand<>'Brand#13'//
    and p_type not like 'ECONOMY BRUSHED%'//消费者不感兴趣的类型和尺寸
    and p_size in(37,49,46,26,11,41,13,21)
    and ps_suppkey not in(//NOT IN子查询，消费者排除某些供货商
        select
            s_suppkey
        from
            supplier
        where
            s_comment like'%Customer%complaints%'
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,//按数量降序排列，按品牌、种类、尺寸升序排列
    p_brand,
    p_type,
    p_size
LIMIT 10;
```

统计每种品牌、类型和尺寸的部件有多少个不同的供应商：

1. 从 `part` 和 `partsupp` 表中筛选出不属于品牌 `Brand#13` 且类型不以 `ECONOMY BRUSHED` 开头的部件，并且部件尺寸在指定的集合内。
2. 排除掉 `supplier` 表中带有特定注释（`%Customer%complaints%`）的供应商。
3. 根据品牌、类型和尺寸进行分组，并计算每组中不同供应商的数量。

添加索引建议：

```sql
create index idx_part_brand_type_size on part (p_brand, p_type, p_size) where p_brand <> 'Brand#13' and p_type not like 'ECONOMY BRUSHED%';
```

### 17.sql 小额订单收入查询

```sql
select
    sum(l_extendedprice)/7.0 as avg_yearly
from
    lineitem,
    part
where
    p_partkey = l_partkey
    and p_brand ='Brand#35'//指定品牌
    and p_container='JUMBO PKG'//指定包装类型
    and l_quantity<( //聚集子查询
        select
            0.2* avg(l_quantity)
        from
            lineitem
        where
            l_partkey =p_partkey
    )
LIMIT 10;
```

计算在满足特定条件下的 `lineitem` 表中 `p_brand` 为 `'Brand#35'` 且 `p_container` 为 `'JUMBO PKG'` 的产品的平均年销售量：

`p_brand = 'Brand#35'`
`p_container = 'JUMBO PKG'`

`l_quantity < (子查询条件)`：子查询中计算了 `avg(l_quantity)`，这可能会影响 `l_quantity` 的过滤。

`p_partkey = l_partkey`：关联条件。

添加索引建议：

```sql
create index i14 on part (p_partkey) where p_brand = 'Brand#35' and p_container = 'JUMBO PKG';
create index i15 on lineitem (l_partkey) include (l_quantity);
```

### 18.sql 大批量客户查询

```sql
select 
  c_name, 
  c_custkey, 
  o_orderkey, 
  o_orderdate, 
  o_totalprice, 
  sum(l_quantity) //订货总数
from 
  customer, 
  orders, 
  lineitem 
where 
  o_orderkey in (//带有分组操作的IN子查询
    select 
      l_orderkey 
    from 
      lineitem 
    group by 
      l_orderkey 
    having 
      sum(l_quantity) > 315
  ) 
  and c_custkey = o_custkey 
  and o_orderkey = l_orderkey 
group by 
  c_name, 
  c_custkey, 
  o_orderkey, 
  o_orderdate, 
  o_totalprice 
order by 
  o_totalprice desc, 
  o_orderdate;
```

主要目的是找出满足特定条件的客户订单，并对这些订单进行统计和排序：

从 `lineitem` 表中选择订单编号 `l_orderkey`，并按 `l_orderkey` 分组。

仅保留那些 `l_quantity` 的总和大于 315 的订单，即订单中商品的总数量超过 315 的订单。

索引创建推荐：

```sql
-- 在 lineitem 表的 l_orderkey 创建索引，并包括l_quantity
create index i16 on lineitem (l_orderkey) include (l_quantity);
-- 在 orders 表的 o_orderkey 和 o_custkey 上创建索引
create index idx_orders_orderkey_custkey on orders (o_orderkey, o_custkey);
-- 在 customer 表的 c_custkey 上创建索引
create index idx_customer_custkey on customer (c_custkey);
```

### 19.sql 折扣收入查询

```sql
select
    sum( l_extendedprice* ( 1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#45'
        and p_container in ( 'SM CASE', 'SM BOX ' , 'SM PACK ' ,'SM PKG')//包装范围
        and l_quantity >= 2 and l_quantity <= 2 + 10
        and p_size between 1 and 5//尺寸范围
        and l_shipmode in ( 'AIR','AIR REG')//运输模式
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        P_partkey = l_partkey
        and p_brand = 'Brand#53'
        and p_container in ( 'MED BAG","MED BOX','MED PKG','MED PACK ')
        and l_quantity >= 13 and l_quantity <=13 + 10
        and p_size between 1 and 10
        and l_shipmode in ( 'AIR','AIR REG' )
        and l_shipinstruct = 'DELIVER IN PERSON '
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#14'
        and p_container in ( 'LG CASE", "LG BOx', 'LG PACK ','LG PKG')
        and l_quantity >= 22 and l_quantity <= 22 + 10
        and p_size between 1 and 15
        and l_shipmode in ( 'AIR','AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
LIMIT 10;
```

计算在特定条件下的销售收入（`revenue`），并通过筛选和分组来获取每种产品组合的销售额:

产品品牌、容器类型和尺寸符合特定条件。

订单数量在给定范围内。

配送方式为 'AIR' 或 'AIR REG'。

配送指示为 'DELIVER IN PERSON'。

创建索引推荐：

```sql
create index idx_lineitem_filter on lineitem (l_partkey, l_shipinstruct, l_quantity, l_shipmode);
CREATE INDEX idx_part_filter ON part (p_partkey,p_brand,p_container,p_size);
```

### 20.sql 潜在零部件促销查询

```sql
select
    s_name,
    s_address
from
    supplier,
    nation
where
    s_suppkey in( //第一层的IN子查询
        select
            ps_suppkey
        from
            partsupp
        where
            ps_partkey in( //第二层嵌套的IN子查询
                select
                    p_partkey
                from
                    part
                where
                    p_name like 'wheat%'
            )
            and ps_availqty >(//第二层嵌套的子查询
                select
                    0.5*sum(l_quantity)//聚集子查询
                from
                    lineitem
                where
                    l_partkey =ps_partkey
                    and l_suppkey=ps_suppkey
                    and l_shipdate >= date'1997-01-01'
                    and l_shipdate<date '1997-01-01'+interval '1'year
            )
    )
    and s_nationkey=n_nationkey
    and n_name ='JAPAN'
order by
    s_name
LIMIT 10;
```

查找来自日本（`n_name = 'JAPAN'`）的供应商的名称（`s_name`）和地址（`s_address`），这些供应商的某些零件（`ps_partkey`）的可用数量（`ps_availqty`）大于其在一年的时间内供应的相应零件的总数量的一半：

查找所有名字以 `wheat` 开头的零件（`p_name like 'wheat%'`）。

找出这些零件的供应商（`ps_suppkey`）。

比较供应商的可用数量（`ps_availqty`）与其在过去一年的供应数量的 0.5 倍，确保可用数量大于此值。

将符合条件的供应商与 `nation` 表连接，以确保其位于日本。

添加索引推荐：

```sql
create index i17 on lineitem (l_partkey,l_suppkey) where l_shipdate >= date '1997-01-01' and l_shipdate < date '1997-01-01' + interval '1' year;
create index i18 on part (p_partkey) where p_name like 'wheat%';

create index idx_partsupp_partkey_suppkey on partsupp (ps_partkey, ps_suppkey, ps_availqty);
create index idx_supplier_suppkey_nationkey on supplier (s_suppkey, s_nationkey);
create index idx_nation_name on nation (n_name);
```

### 21.sql 供应商留单等待查询

```sql
select
    s_name,
    count(*)as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
    s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
    and o_orderstatus='F'
    and l1.l_receiptdate >l1.l_commitdate
    and exists( //EXISTS子查询
        select*
        from
            lineitem l2
        where
            l2.l_orderkey= l1.l_orderkey
            and l2.l_suppkey<>l1.l_suppkey
    )
    and not exists(//NOT EXISTS子查询
        select*
        from
            lineitem l3
        where
            l3.l_orderkey= l1.l_orderkey
            and l3.l_suppkey<>l1.l_suppkey
            and l3.l_receiptdate >l3.l_commitdate
    )
    and s_nationkey =n_nationkey
    and n_name ='RUSSIA'
group by
    s_name
order by
    numwait desc,
    s_name
LIMIT 100;
```

从位于俄罗斯的供应商中找出那些有待交付的订单的供应商名称（`s_name`）和待处理订单数量（`numwait`）:

供应商的订单状态是 "F"（表示已完成）。

订单中有至少一个行项目的 `l_receiptdate` 晚于 `l_commitdate`，即该行项目尚未完成交付。

该订单中有另一个不同的供应商提供了该商品。

同一订单中没有其他行项目在 `l_commitdate` 之后的 `l_receiptdate`，确保该订单没有其他待交付的部分。

添加索引推荐：

```sql
create index i19 on lineitem (l_orderkey) include (l_suppkey) where l_receiptdate <> l_commitdate;
create index i20 on lineitem (l_orderkey) include (l_suppkey);
create index i21 on orders (o_orderkey) where o_orderstatus = 'F';

create index idx_supplier_suppkey_nationkey on supplier (s_suppkey, s_nationkey);
create index idx_nation_name on nation (n_name);
```

### 22.sql 全球销售机会查询

```sql
select
    cntrycode,
    count(*)as numcust,
    sum(c_acctbal)as totacctbal
from //第一层子查询
    (
        select
            substring(c_phone from 1 for 2)as cntrycode,
            c_acctbal
        from
            customer
        where
            substring(c_phone from 1 for 2)in('32','24','40','26','30','28','31')
            and c_acctbal>(//第二层聚集子查询
                select
                    avg(c_acctbal)
                from
                    customer
                where
                    c_acctbal >0.00
                    and substring(c_phone from 1 for 2)in('32','24','40','26','30','28','31')
            )
            and not exists(//第二层NOT EXISTS子查询
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
    cntrycode
LIMIT 10;
```

获取没有在 `orders` 表中出现的客户的信息：

过滤出账户余额大于零且电话号码前两位在指定国家代码列表中的客户。

排除出现在 `orders` 表中的客户（即没有下过订单的客户）。

按国家代码分组，并按国家代码排序。

添加索引推荐：

```sql
CREATE INDEX idx_customer_phone ON public.customer USING btree (substring(c_phone from 1 for 2)); -- 对应条件 substring(c_phone from 1 for 2) in (...)
CREATE INDEX idx_customer_acctbal ON public.customer USING btree (c_acctbal); -- 对应条件 c_acctbal > ...
CREATE INDEX idx_customer_custkey ON public.customer USING btree (c_custkey); -- 用于关联 NOT EXISTS 子查询中的 o_custkey = c_custkey
CREATE INDEX idx_orders_custkey ON public.orders USING btree (o_custkey); -- 对应子查询条件 o_custkey = c_custkey

CREATE INDEX idx_customer_phone_acctbal_custkey ON public.customer (substring(c_phone from 1 for 2), c_acctbal, c_custkey);。
create index idx_customer_phone_acctbal on customer (c_phone, c_acctbal, c_custkey);
create index idx_orders_custkey on orders (o_custkey);
```
