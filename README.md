# big_file_index
实现了一个基于哈希的二进制数据文件索引结构及数据读取接口。<br>
## 假设条件
1. 数据在文件中按照(key_size, key, value_size, value)无序排列；
2. key_size，value_size字段均为两字节；
3. 目标机器CPU按照big endian方式存放数据；
4. 文件slice大小为2GB，且每个文件slice均以一个完整的tuple开头，即不存在跨slice的数据。
## 主要功能介绍
### IndexBuilder类
* IndexBuilder类用于创建索引，采用hashlib库提供的black2b函数，将键值均匀分布在0到2^32之间；
* 每次从数据文件中读取大小为2GB的slice进入内存进行索引的构建；
* 对于可能出现的hash冲突采用链地址的方式解决，冲突链以追加的方式写入文件末尾；
* 待索引构建完成后，对冲突链进行紧缩，去除冲突链之间的的碎片空间。
### DataReader类
* 通过IndexBuilder构建的hash索引进行数据的读取；
* 如果重复插入相同的key，返回最后插入的数据。