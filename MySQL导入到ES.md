# Logstash：把 MySQL 数据导入到 Elasticsearch 中

## 环境要求

- Elasticsearch
- Kibana
- Logstash

## Logstash 配置

logstash.conf 配置示例：（该文件在logstash的安装路径下的config目录，若没有则新建该文件）

```json
input {
  jdbc {
  jdbc_driver_library
  =>
  "E:\IT\environment config\logstash-7.17.5\logstash-core\lib\jars\mysql-connector-java-8.0.20.jar"
  jdbc_driver_class
  =>
  "com.mysql.jdbc.Driver"
  jdbc_connection_string
  =>
  "jdbc:mysql://agilec.gicp.net:20221/ccf41?serverTimezone=GMT%2B8"
  jdbc_user
  =>
  "ccf41"
  jdbc_password
  =>
  "6s2jwgycRn"
  statement
  =>
  "SELECT * from tb_acc_txn LIMIT 2000000, 1000000"
}
}

output {
elasticsearch {
hosts => ["localhost:9200"]
index => "tb_acc_txn"
}
}
```

### 导入所需jar包到logstash

我们需要下载相应的 JDBC connector。该jar包我已经放在项目的resources目录内，即**_mysql-connector-java-8.0.20.jar_**，把这个jar包存入到 Logstash
安装目录下的子目录（logstash-core/lib/jars/）中。

## 说明

    本次比赛用的MySQL表共有5个，需要依次导入到本地的ES中（暂时没有云服务器，先在本地开发）。

    从MySQL导入ES时，要先建index和mapping，再启动logstash导入！
    从MySQL导入ES时，要先建index和mapping，再启动logstash导入！
    从MySQL导入ES时，要先建index和mapping，再启动logstash导入！

**_下面介绍每个表导入时的步骤_**

### 新建tb_acc并导入

首先在kibana的dev-tools内新建tb_acc索引并指定mapping，即在http://localhost:5601/app/dev_tools#/console 中输入以下代码：

```json
# 创建索引时指定映射
PUT /tb_acc
{
"settings": {
"number_of_shards": 1,
"number_of_replicas": 0
},
"mappings": {
"properties": {
"id": {
"type": "integer"
},
"time": {
"type": "date",
"format": "yyyyMMdd",
"ignore_malformed": true,
"null_value": null
},
"head_no": {
"type": "keyword"
},
"bank_code1":{
"type": "keyword"
},
"self_acc_name": {
"type": "keyword"
},
"acc_state": {
"type": "keyword"
},
"self_acc_no": {
"type": "keyword"
},
"card_no": {
"type": "keyword"
},
"acc_type":{
"type": "keyword"
},
"acc_type1": {
"type": "keyword"
},
"id_no": {
"type": "keyword"
},
"cst_no": {
"type": "keyword"
},
"fixed_flag": {
"type": "keyword"
},
"ent_cst_type":{
"type": "keyword"
},
"frg_flag": {
"type": "keyword"
},
"open_time": {
"type": "date",
"format": "yyyyMMdd",
"ignore_malformed": true,
"null_value": null
},
"close_time": {
"type": "date",
"format": "yyyyMMdd",
"ignore_malformed": true,
"null_value": null
},
"acc_flag": {
"type": "keyword"
},
"credit_flag": {
"type": "keyword"
},

"w_type": {
"type": "keyword"
},
"bank_tel":{
"type": "keyword"
},
"open_type": {
"type": "keyword"
},
"open_type1": {
"type": "keyword"
},
"agent_name": {
"type": "keyword"
},
"agent_tel": {
"type": "keyword"
},
"agent_type":{
"type": "keyword"
},
"agent_no": {
"type": "keyword"
},
"extend1": {
"type": "keyword"
},
"extend2": {
"type": "keyword"
}
}
}
}
```

然后修改``logstash.conf``文件 （在logstash安装目录下的config目录内，**若没有则新建该文件**）。

``logstash.conf``文件配置如下：

```json
input {
  jdbc {
  jdbc_driver_library
  =>
  "E:\IT\environment config\logstash-7.17.5\logstash-core\lib\jars\mysql-connector-java-8.0.20.jar"
  jdbc_driver_class
  =>
  "com.mysql.jdbc.Driver"
  jdbc_connection_string
  =>
  "jdbc:mysql://agilec.gicp.net:20221/ccf41?serverTimezone=GMT%2B8"
  jdbc_user
  =>
  "ccf41"
  jdbc_password
  =>
  "6s2jwgycRn"
  statement
  =>
  "SELECT * from tb_acc"
}
}

output {
elasticsearch {
hosts => ["localhost:9200"]
index => "tb_acc"
}
}
```

**注意事项：**

- 配置信息中的``jdbc_driver_library``路径，改成你自己的路径
- 配置信息的``statement``中的表名要和本次导入的表名对应
- 配置信息中的``index``要和本次导入的索引对应

### 新建tb_acc_txn并导入

首先在kibana的dev-tools内新建tb_acc_txn索引并指定mapping，即在http://localhost:5601/app/dev_tools#/console 中输入以下代码：

```json
## 创建索引时指定映射 date2为交易日期
PUT /tb_acc_txn
{
"settings": {
"number_of_shards": 1,
"number_of_replicas": 0
},
"mappings": {
"properties": {
"id": {
"type": "long"
},
"hour": {
"type": "date",
"format": "yyyyMMdd",
"ignore_malformed": true,
"null_value": null
},
"date": {
"type": "date",
"format": "yyyy-MM-dd HH:mm:ss",
"ignore_malformed": true,
"null_value": null
},
"date2": {
"type": "date",
"format": "yyyy-MM-dd",
"ignore_malformed": true,
"null_value": null
},
"time": {
"type": "date",
"format": "HHmmss",
"ignore_malformed": true,
"null_value": null
},
"self_bank_code": {
"type": "keyword"
},
"acc_type": {
"type": "keyword"
},
"cst_no":{
"type": "keyword"
},
"id_no": {
"type": "keyword"
},
"self_acc_no": {
"type": "keyword"
},
"card_no": {
"type": "keyword"
},
"self_acc_name": {
"type": "keyword"
},
"acc_flag":{
"type": "keyword"
},
"part_bank_code": {
"type": "keyword"
},
"part_bank_name": {
"type": "keyword"
},
"part_acc_no": {
"type": "keyword"
},
"part_acc_name": {
"type": "keyword"
},
"lend_flag":{
"type": "keyword"
},
"tsf_flag": {
"type": "keyword"
},
"cur": {
"type": "keyword"
},
"org_amt": {
"type": "double"
},
"usd_amt": {
"type": "double"
},
"rmb_amt":{
"type": "double"
},
"balance": {
"type": "double"
},
"agency_flag": {
"type": "keyword"
},
"agent_name": {
"type": "keyword"
},
"agent_tel": {
"type": "keyword"
},
"agent_type":{
"type": "keyword"
},
"agent_no": {
"type": "keyword"
},
"ticd": {
"type": "keyword"
},
"counter_no": {
"type": "keyword"
},
"settle_type": {
"type": "keyword"
},
"reverse_flag":{
"type": "keyword"
},
"purpose": {
"type": "keyword"
},
"bord_flag": {
"type": "keyword"
},
"nation": {
"type": "keyword"
},
"bank_flag": {
"type": "keyword"
},
"ip_code":{
"type": "keyword"
},
"atm_code": {
"type": "keyword"
},
"bank_code": {
"type": "keyword"
},
"mac_info": {
"type": "keyword"
},
"extend1": {
"type": "keyword"
},
"extend2":{
"type": "keyword"
}
}
}
}
```

然后修改``logstash.conf``文件 （在logstash安装目录下的config目录内，**若没有则新建该文件**）。

``logstash.conf``文件配置如下：

```json
input {
  jdbc {
  jdbc_driver_library
  =>
  "E:\IT\environment config\logstash-7.17.5\logstash-core\lib\jars\mysql-connector-java-8.0.20.jar"
  jdbc_driver_class
  =>
  "com.mysql.jdbc.Driver"
  jdbc_connection_string
  =>
  "jdbc:mysql://agilec.gicp.net:20221/ccf41?serverTimezone=GMT%2B8"
  jdbc_user
  =>
  "ccf41"
  jdbc_password
  =>
  "6s2jwgycRn"
  statement
  =>
  "SELECT * from tb_acc_txn"
}
}

output {
elasticsearch {
hosts => ["localhost:9200"]
index => "tb_acc_txn"
}
}
```

**注意事项：**

- 配置信息中的``jdbc_driver_library``路径，改成你自己的路径
- 配置信息的``statement``中的表名要和本次导入的表名对应
- 配置信息中的``index``要和本次导入的索引对应



### 新建tb_cred_txn并导入

首先在kibana的dev-tools内新建tb_cred_txn索引并指定mapping，即在http://localhost:5601/app/dev_tools#/console 中输入以下代码：

```json
# 创建索引时指定映射
PUT /tb_cred_txn
{
"settings": {
"number_of_shards": 1,
"number_of_replicas": 0
},
"mappings": {
"properties": {
"self_acc_no": {
"type": "keyword"
},
"card_no": {
"type": "keyword"
},
"self_acc_name":{
"type": "keyword"
},
"cst_no": {
"type": "keyword"
},
"id_no": {
"type": "keyword"
},
"date": {
"type": "date",
"format": "yyyyMMdd",
"ignore_malformed": true,
"null_value": null
},
"time": {
"type": "date",
"format": "HHmmss",
"ignore_malformed": true,
"null_value": null
},
"lend_flag": {
"type": "keyword"
},
"tsf_flag": {
"type": "keyword"
},
"cur":{
"type": "keyword"
},
"org_amt": {
"type": "double"
},
"usd_amt": {
"type": "double"
},
"rmb_amt": {
"type": "double"
},
"balance": {
"type": "double"
},
"purpose":{
"type": "keyword"
},
"pos_owner": {
"type": "keyword"
},
"trans_type": {
"type": "keyword"
},
"ip_code": {
"type": "keyword"
},
"bord_flag": {
"type": "keyword"
},
"nation":{
"type": "keyword"
},
"part_bank": {
"type": "keyword"
},
"part_acc_no": {
"type": "keyword"
},
"part_acc_name": {
"type": "keyword"
},
"settle_type": {
"type": "keyword"
},
"ticd":{
"type": "keyword"
},
"reverse_flag": {
"type": "keyword"
},
"pos_code": {
"type": "keyword"
},
"atm_code": {
"type": "keyword"
},
"mac_info": {
"type": "keyword"
}
}
}
}

```

然后修改``logstash.conf``文件 （在logstash安装目录下的config目录内，**若没有则新建该文件**）。

``logstash.conf``文件配置如下：

```json
input {
  jdbc {
  jdbc_driver_library => "E:\IT\environment config\logstash-7.17.5\logstash-core\lib\jars\mysql-connector-java-8.0.20.jar"
  jdbc_driver_class => "com.mysql.jdbc.Driver"
  jdbc_connection_string => "jdbc:mysql://agilec.gicp.net:20221/ccf41?serverTimezone=GMT%2B8"
  jdbc_user => "ccf41"
  jdbc_password => "6s2jwgycRn"
  statement => "SELECT * from tb_cred_txn"
}
}

output {
elasticsearch {
hosts => [ "localhost:9200" ]
index => "tb_cred_txn"
}
}
```

**注意事项：**

- 配置信息中的``jdbc_driver_library``路径，改成你自己的路径
- 配置信息的``statement``中的表名要和本次导入的表名对应
- 配置信息中的``index``要和本次导入的索引对应



### 新建tb_cst_pers并导入

首先在kibana的dev-tools内新建tb_cst_pers索引并指定mapping，即在http://localhost:5601/app/dev_tools#/console 中输入以下代码：

```json
# 创建索引时指定映射
PUT /tb_cst_pers
{
"settings": {
"number_of_shards": 1,
"number_of_replicas": 0
},
"mappings": {
"properties": {
"head_no":{
"type": "keyword"
},
"bank_code1":{
"type": "keyword"
},
"cst_no":{
"type": "keyword"
},
"open_time":{
"type": "date",
"format": "yyyyMMdd"
},
"close_time":{
"type": "keyword"
},
"acc_name":{
"type": "keyword"
},
"cst_sex":{
"type": "keyword"
},
"nation":{
"type": "keyword"
},
"id_type":{
"type": "keyword"
},
"id_no":{
"type": "keyword"
},
"id_deadline":{
"type": "keyword"
},
"occupation":{
"type": "keyword"
},
"income":{
"type": "double"
},
"contact1":{
"type": "keyword"
},
"contact2":{
"type": "keyword"
},
"contact3":{
"type": "keyword"
},
"address1":{
"type": "keyword"
},
"address2":{
"type": "keyword"
},
"address3":{
"type": "keyword"
},
"company":{
"type": "keyword"
},
"sys_name":{
"type": "keyword"
},
"dml_date":{
"type": "keyword"
},
"dml_code":{
"type": "keyword"
},
"cst_yn":{
"type": "keyword"
},
"extend1":{
"type": "keyword"
},
"extend2":{
"type": "keyword"
},
"id":{
"type": "long"
},
"is_smkh":{
"type": "keyword"
},
"is_tskh":{
"type": "keyword"
},
"is_wjhke":{
"type": "keyword"
},
"is_ybsbkh":{
"type": "keyword"
}
}
}
}

```

然后修改``logstash.conf``文件 （在logstash安装目录下的config目录内，**若没有则新建该文件**）。

``logstash.conf``文件配置如下：

```json
input {
  jdbc {
  jdbc_driver_library => "E:\IT\environment config\logstash-7.17.5\logstash-core\lib\jars\mysql-connector-java-8.0.20.jar"
  jdbc_driver_class => "com.mysql.jdbc.Driver"
  jdbc_connection_string => "jdbc:mysql://agilec.gicp.net:20221/ccf41?serverTimezone=GMT%2B8"
  jdbc_user => "ccf41"
  jdbc_password => "6s2jwgycRn"
  statement => "SELECT * from tb_cst_pers"
}
}

output {
elasticsearch {
hosts => [ "localhost:9200" ]
index => "tb_cst_pers"
}
}
```

**注意事项：**

- 配置信息中的``jdbc_driver_library``路径，改成你自己的路径
- 配置信息的``statement``中的表名要和本次导入的表名对应
- 配置信息中的``index``要和本次导入的索引对应



### 新建tb_cst_unit并导入

首先在kibana的dev-tools内新建tb_cst_unit索引并指定mapping，即在http://localhost:5601/app/dev_tools#/console 中输入以下代码：

```json
# 创建索引时指定映射
PUT /tb_cst_unit
{
"settings": {
"number_of_shards": 1,
"number_of_replicas": 0
},
"mappings": {
"properties": {
"time":{
"type": "date",
"format": "yyyyMMdd",
"ignore_malformed": true,
"null_value": null
},
"head_no":{
"type": "keyword"
},
"bank_code1":{
"type": "keyword"
},
"cst_no":{
"type": "keyword"
},
"open_time":{
"type": "date",
"format": "yyyyMMdd",
"ignore_malformed": true,
"null_value": null
},
"acc_name":{
"type": "keyword"
},
"address":{
"type": "keyword"
},
"operate":{
"type": "keyword"
},
"set_file":{
"type": "keyword"
},
"license":{
"type": "keyword"
},
"id_deadline":{
"type": "date",
"format": "yyyyMMdd",
"ignore_malformed": true,
"null_value": null
},
"org_no":{
"type": "keyword"
},
"tax_no":{
"type": "keyword"
},
"rep_name":{
"type": "keyword"
},
"id_type2":{
"type": "keyword"
},
"id_no2":{
"type": "keyword"
},
"id_deadline2":{
"type": "date",
"format": "yyyyMMdd",
"ignore_malformed": true,
"null_value": null
},
"man_name":{
"type": "keyword"
},
"id_type3":{
"type": "keyword"
},
"id_no3":{
"type": "keyword"
},
"id_deadline3":{
"type": "date",
"format": "yyyyMMdd",
"ignore_malformed": true,
"null_value": null
},
"ope_name":{
"type": "keyword"
},
"id_type4":{
"type": "keyword"
},
"id_no4":{
"type": "keyword"
},
"id_deadline4":{
"type": "date",
"format": "yyyyMMdd",
"ignore_malformed": true,
"null_value": null
},
"industry":{
"type": "keyword"
},
"reg_amt":{
"type": "keyword"
},
"code":{
"type": "keyword"
},
"sys_name":{
"type": "keyword"
},
"dml_date":{
"type": "keyword"
},
"dml_code":{
"type": "keyword"
},
"cst_yn":{
"type": "keyword"
},
"ywjl_time":{
"type": "keyword"
},
"ywjs_time":{
"type": "keyword"
},
"is_jxkh":{
"type": "keyword"
},
"extend1":{
"type": "keyword"
},
"extend2":{
"type": "keyword"
}
}
}
}

```

然后修改``logstash.conf``文件 （在logstash安装目录下的config目录内，**若没有则新建该文件**）。

``logstash.conf``文件配置如下：

```json
input {
  jdbc {
  jdbc_driver_library => "E:\IT\environment config\logstash-7.17.5\logstash-core\lib\jars\mysql-connector-java-8.0.20.jar"
  jdbc_driver_class => "com.mysql.jdbc.Driver"
  jdbc_connection_string => "jdbc:mysql://agilec.gicp.net:20221/ccf41?serverTimezone=GMT%2B8"
  jdbc_user => "ccf41"
  jdbc_password => "6s2jwgycRn"
  statement => "SELECT * from tb_cst_unit"
}
}

output {
elasticsearch {
hosts => [ "localhost:9200" ]
index => "tb_cst_unit"
}
}
```

**注意事项：**

- 配置信息中的``jdbc_driver_library``路径，改成你自己的路径
- 配置信息的``statement``中的表名要和本次导入的表名对应
- 配置信息中的``index``要和本次导入的索引对应