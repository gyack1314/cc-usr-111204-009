# 园区遥测MQTT二阶段交付的持久收集

此Python项目管理独立的能源领域合成源资料，`data/`为原始样本及访问配置，`source_data.io`负责严格读取和字节身份，`source_data.domain`提供现有库存摘要，`source_data.replay`按声明顺序返回不解释的源响应。字段和支持的来源口径见[资料约定](docs/data-contract.md)。所有机构、设备、对象与数值均为虚构。

Python 3.11及以上即可使用，无第三方Python依赖。项目根目录运行：

```sh
python -m unittest discover -s tests -v
python -m compileall -q source_data tests
python -m source_data.cli inspect data/normal.json
```

`inspect`展示已有的原始库存，不修改资料。`invalid-`前缀文件为不合法输入，其他变体保留不同业务边界；源协议与二进制格式使用资料约定中的有限范围，未解释部分仍以原字节保存。

## QoS2入站持久收集器

`receiver/`在上述合成事件之上实现MQTT5 QoS2入站交付状态机：按会话世代与packet_id管理在途交换，PUBLISH先在本地事务持久化完整topic、payload、properties与接收身份再发PUBREC，PUBREL在一个事务内把样本标记为正式可见并完成交换再发PUBCOMP；确认意图先记账后经虚拟通道发出。设计与崩溃/重演语义见[接收器设计](docs/receiver-design.md)。

```sh
python -m unittest discover -s tests -v
# 离线回放，输出正式样本、确认顺序、世代与交换状态
python -m receiver.cli replay data/normal.json
# 在指定事件（0基）的某个持久化/发送边界注入进程退出，并核对从原始日志重建的状态
python -m receiver.cli rebuild data/lost-session.json --crash-event 3 --crash-point after_pubcomp_send
```

崩溃边界取值见`receiver.engine.CRASH_POINTS`（事务提交前后、PUBREC/PUBCOMP发送前后共八个）。`receiver.replay.CrashPlan`支持在一条feed的多个边界各退出一次；恢复后确认可重发而正式样本只出现一次，且`rebuild_from_journal`能从原始消息逐表重建出相同状态。接收器不联网、不编码报文、不操控设备。

固定基础镜像见Dockerfile，可执行`docker build -t energy-source .`，再用`docker run --rm energy-source`显示摘要；`docker run --rm energy-source python -m unittest discover -s tests -v`执行已有检查。资料和回放不依赖真实设备、账户或实时网络服务。
