# 概述

一个策略完整的生命周期包括
* 研究： 根据历史数据（通常是价格数据），找到统计显著性，可以是显著的相关性，
也可以是显著的分类差异。 大部分尝试都是没有结果的，只有很少的能走到下一步
* 回测： 回测应该是模拟真实的环境，如果回测是在理想环境中进行的，其实其结果
是可以在研究那一部分完成的（复杂的策略除外）。实践中，很多现实环境中会碰到的问题在
回测过程中都没有暴露出来，回测只帮忙验证了一些在单元测试阶段应该验证的程序逻辑，
其验证方法就是比对回测的累计收益跟研究过程的累计收益。 实践中，很多时候成交价格
跟理想价格之间存在比较大的差距，且由于交易时间的约束，回测和实盘环境下交易信号可能
会不同，因为在回测中是严格开盘和收盘时刻的价格，但是在实盘的时候不会在收盘的时刻下单
因为订单可能不成交，所以会有提前。
* 模拟盘和实盘： 有些券商不提供模拟盘的功能，比如德美利。 模拟盘跟实盘在实现上是一样
的，就是一个账户标识的区别。 实盘应该尽量跟策略匹配，因为如果策略是有效的，但是因为实现
上的问题导致错过好的交易信号或者发出了错误的交易信号，是很难受的。
  

关于目录结构
* 每一个策略在该目录下有一个自己的目录，策略代码放在策略目录下，策略代码一份，但是回测、实盘都共用同一份策略代码，
  实盘的配置根据运行的机器、使用的券商会有不同的配置，策略生命周期节点与运行的机器与使用的券商的不同组合，形成一个运行
  时，每一个运行时对应一个目录，每一个运行时有自己的配置文件。

关于策略逻辑的测试
* 如上述所说，策略在理想情况下的运行情况可以通过回测来测试，理想情况指的是无限成交量以及订单服务的高可用。而实际市场
是有流动性限制的，这意味着
  * 单子不能立即成交（特定时刻的单子需要提前）
  * 成交价格会偏离理想的价格
  * 如果不想使用市价单的话，可能需要议价算法，这是一个非常难的领域，前期需要记录下来足够多的数据以用于后续的分析
  * 券商的订单服务可能不可用


关于议价算法
* 回测应该尽量模拟实盘， 策略是构建在理想的情况之上（即无限成交量，所有订单可以立即撮合成功），
实际情况成交量是受到限制的，一个订单的成交通常是一个议价的过程，其实现是基于当前的实时价格
以及目标价格，设置一个初始的价格，并且根据后面的实时价格走势，来动态的对价格进行调整。这个
议价的算法以及当时的市场走势，会直接影响到你的成交价，这个价格通常跟目标价格有不少的差距，
这可以称之为滑点，可以通过很多订单的滑点的平均值指标来优化议价算法。 甚至于这个议价算法都没有市价单来的有效
使用带有议价算法的限价单或者使用市价单都是为了不丧失机会成本，不错过交易信号。相当于是在成交
和成本的权衡中选择了成交（某些卖空的情况可能因为没有空池而不能成交）。
* 一个议价算法有效，一定是这个议价算法能够战胜市价单，当时使用赢透作为自己券商的时候，因为
市价单会打来更多的佣金，所以就决定使用了限价单，使用限价单后滑点很高，使用德美利券商之后，可以考虑试一下市价单
* 在实现上，订单可以关联一个议价对象，一个议价对象包含从议价开始到结束的过程中实时价格的变化历史，以及价格的调整
历史。议价对象有一个实例方法，即议价算法，这可以通过多态的方式来实现，议价算法基于最新的价格历史以及当前的价格，来决定
是否需要进行价格的调整。 为了获得最新的实时价格，议价算法需要注册为时序数据监听者。调用一个带有议价对象的订单的下单操作 
会启动议价过程，订单状态变为终态时议价过程结束。 
  
带有实例方法的序列化和反序列化
