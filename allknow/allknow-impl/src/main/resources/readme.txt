此服务是一个内存的全文索引

插入 list of (id -> keyword)
支持高效查询keyword->Ids的模糊查询
比如， 查询广告名字包含"全国"的广告IDs

核心算法是后缀索引(Manber's algorithm)
通过数据分区和"热交换"，来避免静态索引带来的重建索引的长时间阻塞


其中:
Manber.java :  后缀索引核心算法
KWIC.java   :  全文索引的基本接口和方法
HotSwapKWIC.java  : 组合KWIC， 得到交换的索引
MonitorIndex.java : 组合HotSwapKWIC, 启动后台替换索引的线程

BigIndexLoadTest, MediumIndexTest, SmallIndexTest 对正确性验证
SwitchIndexProfilingTest 验证索引替换不影响查询效率
