﻿1、pydis 是基于 python 3.x 版本实现的 redis 客户端，代码是基于 redis 协议的基本实现，没有优化重构，便于基本原理的理解学习。不能用于生产环境。

2、实现了 redis 基本的命令发送接受功能（目前只实现了 30 多个，命令实在是太多了，命令处理方式基本是一致的）。

3、实现了 pipeline 功能和发布/订阅功能。但是对返回结果没有进行包装（没有解析结果前缀和绑定命令）。

4、实现了 cluster 中的 moved 指令，但是没有实现 ask 指令（ask 是在访问的 slot 转移过程中触发的，没有模拟出对应环境）。

5、本代码仅用于 redis 通信协议的学习研究。如有问题，欢迎拍砖：maphey@qq.com。