# 职责说明

外存分组汇总按契约拆为五层，`spill.py` 只做兼容转发，真实业务只走
`pipeline.aggregate` 一条执行链。

## 模块职责与状态归属

- **reduction.py（纯归约核）**：`Rules` 提供 `lift/key/combine/finish`，只含业务
  归约规则。无实例状态、无可变全局状态，不接触文件、工作区、句柄或 I/O 回调；
  `finish` 是惰性生成器，仅持当前组的常数累积量。NULL 设备不计、空字符串设备
  计数、跨段同设备经同键合并后只计一次，这些语义全部归属此层。
- **sorter.py（归并调度）**：`order` 负责限额切块（至多 `rows_per_run` 单元）、
  排序、分轮分批、堆归并（至多 `fanin` 个读头加常数归约单元）及拉取顺序。
  状态仅为段名列表、当前块、堆与不透明令牌；不持物理句柄，不解释记录或单元
  字段（只经 `rules.lift/key/combine`），不调用 `finish`，不关闭原始输入，
  也不在正常产出结束时做资源清理。
- **driver.py（文件资源驱动）**：`Driver` 独占真实 Handle、活动句柄登记表
  （令牌→Handle）与待删段所有权列表。`allocate` 登记所有权；显式 `close/remove`
  先释放登记再执行，注入故障也不会造成重复释放；`cleanup` 逆序逐项清理仍登记
  的句柄与段，按发生顺序收集异常返回，不替换主异常。不比较键、不归约、不遍历
  单元流。
- **pipeline.py（装配与生命周期）**：调用时校验 `rows_per_run/fanin`，装配
  `Rules/Driver/order`，拥有输入迭代器与最终输出的生命周期及异常优先级：
  处理中的异常优先于清理异常，正常结束时抛清理首个异常，显式 `close` 的
  GeneratorExit 路径清理故障不外抛。
- **spill.py**：仅 `from pipeline import aggregate`，无旧闭包、无备用执行器。
- **cleanup.py**：契约允许的通用清理工具（按序执行、收集异常、按需上抛）。

## 最后一组交付后的资源交接

`finish` 交付最后一组时，最终归并的读句柄通常已随段耗尽而逆序关闭，但输入
迭代器仍未关闭、最终段仍登记在 `Driver` 的所有权列表中——`order` 从不删除
最终段，也不调用 `cleanup`。调用方再拉取一次（或异常、显式 `close`）时，
`pipeline` 的 `finally` 按序交接：

1. 关闭输入迭代器（即使早已到 EOF）；
2. 关闭 `order` 生成器，连带关闭最终归并的遗留读句柄；
3. `driver.cleanup()` 逆序关闭遗留句柄、逆序删除仍归本次作业的段，逐项继续
   并收集异常；
4. 已有处理异常优先；仅正常结束时抛出清理中的首个异常。

由此 `live` 与 `remaining` 在结束、失败或 `close` 后均为空，事件轨迹、每次
拉取后的轨迹长度与异常优先级与重构前逐条一致（由 `expected.json` 冻结核验）。

## 验证入口

- `python3 verify_all.py`（运行时 verify）：原 `checks.py verify` 行为回归 +
  `checks.py disk` 真实磁盘对照 + `structure.py` 结构验证（纯规则、替身驱动
  调度、故障设施下的 Driver、导入与调用边界）。
- `python3 checks.py disk`（运行时 demo）：真实临时文件上的结果与完整轨迹展示。
