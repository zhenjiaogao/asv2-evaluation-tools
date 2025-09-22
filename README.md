```
注意：
1. 该工具仅从数学的逻辑对成本进行估算，非实际账单成本。
2. 该工具仅从成本的维度出发，未考虑性能。
```

## 简介

通过自动化的方式采集目标 Region 下的数据库实例的 CPU 使用率，Throughput 以及 Latency 等基础数据，以及实例价格信息，分别从成本以及性能的角度帮助客户评估 Provision 实例迁移到 ASv2 的可行性。ASv2 Evaluation Tool 以 CPU 利用率为主要判断依据，从成本的维度实现评估，并产出报表。

当前版本仅支持 RDS MySQL/Postgresql 和 Aurora MySQL/Postgresql 四种实例。

## 架构
调用 DescribeDBInstances API，Cloud Watch API 以及 Price API，采集 31 天内的 CPU 利用率，实例规格，以及成本等信息，从 Privison 以及 ASv2 两个维度输出成本及对比结果，以及基于该 CPU 使用特征下的推荐的 Min ACU 的值。
<img width="684" height="328" alt="image" src="https://github.com/user-attachments/assets/7685c52f-9bc4-49b1-82cd-cda63fbb183c" />

## 资源准备及初始化环境
1. 资源准备
* 1 台 EC2，1 核以上 ( 如 t2.micro)，Amazon 操作系统；与待评估数据库处于同一网络环境，或可访问到对应的 CloudWatch 服务
* EC2 设置 API 访问权限，创建 IAM role，并授权
  * AWSQuickSightDescribeRDS
  * AWSPriceListServiceFullAccess
  * CloudWatchReadOnlyAccess
2. 初始化环境
* 安装 Python 3.8+：
  
  ```yum install python3.8```
  
* 安装以下 4 个 Python 库：

  ```
  yum install -y python3-pip
  pip3 install boto3
  pip3 install pandas
  pip3 install openpyxl
  pip3 install matplotlib
  
  python3 -c 'import boto3;print("ok")'
  python3 -c 'import pandas;print("ok")'
  python3 -c 'import openpyxl;print("ok")'
  python3 -c 'import matplotlib;print("ok")'
  ```
## 执行脚本
1. Clone 脚本
```
git clone https://github.com/zhenjiaogao/asv2-evaluation-tools.git
```
2. 执行
以 ASv2 Evaluation Tool 为例，运行并按照提示输入目标 Region:
```
python3 Aurora_ASv2_evaluation_tools_for_global_region.py 
```
<img width="493" height="431" alt="image" src="https://github.com/user-attachments/assets/8ddfeef4-26dd-4265-8ae8-8065544a19da" />

3. 在执行完脚本后，会在脚本所在目录下，生成对应的结果文件
<img width="988" height="155" alt="image" src="https://github.com/user-attachments/assets/1af9065c-989d-4f5b-a0fb-0eb8bf2da5e1" />

  * ASv2 Evaluation Tool
      * asv2_evaluation_report.xlsx 为 ASv2 成本评估报表
      * rds_explorer_log_xxxx.log日志文件，记录运行过程中的异常信息，帮助排错
  * ASv2 Performance Snapshot Tool：instance_metrics.csv 为数据库实例性能指标信息
    
## 字段释义
| 字段名称 | 说明 |
|---------|------|
| account_id | 账户 ID |
| region | Region |
| instance id | 标识 |
| engine | 数据库引擎 |
| engine_version | 数据库引擎版本 |
| instance type | 规格 |
| vcpu | vcpu 数 |
| CPU Avg Util% | 平均 cpu |
| CPU Min Util% | 最小 cpu |
| CPU Max Util% | 最大 cpu |
| StartTime | CPU 资源数据取到的最早时间 |
| EndTime | CPU 资源数据取到的最近时间 |
| Ondemand/monthly | OD 单月成本 |
| 1 YR NP/monthly | 1 年 RI no upfront 单月成本 |
| 3 YR AP/monthly | 3 年 RI all upfront 单月成本 |
| Min ACU | 建议的最小 ACU |
| ASv2 Price/h | ACU 单价 |
| ASV2 Cost 1/monthly | avg-cpu-utilization * vcpu * 4 * 730 * ACU 单价 |
| ASV2 Cost 2/monthly | (minacu+exceedminacu) * 730 * ACU 单价 |
| Save Percent 1 | ASV2 Cost 1 比 1 YR NP 成本低的百分比 |
| Save Percent 2 | ASV2 Cost 2 比 1 YR NP 成本低的百分比 |

## FAQ
1. 工具使用了哪些API调用，是否会产生相关费用？
     主要使用 Pricing 和 CloudWatch 模块的API：
     
     * pricing.get_products
     * cloudwatch.get_metric_data
     * cloudwatch.get_metric_statistics
     
     其中 cloudwatch.get_metric_data以及 cloudwatch.get_metric_statistics 每 1000 个请求的指标 USD 0.01 ，其他API请求 100 万次以内调用免费。更多 API 定价参考：
     https://aws.amazon.com/cn/cloudwatch/pricing/?nc1=h_ls
