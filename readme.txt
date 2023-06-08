
========================项目说明=========================
该项目调用Pylucenne进行索引文件的写入和索引文件的搜索

======================文件夹说明=======================

/root/data：这是挂载的bj3090 /data10t01/RMW/RMW_data_pro目录

/root/jieba-master：这是jieba中文分词工具

/root/node_index_dir：这是ABC节点的索引文件，
在/data10t01/RMW/RMW_data_pro/index_dir/node_index有备份，但请勿修改

/root/para_index_dir：这是段落内容的索引文件，
在/data10t01/RMW/RMW_data_pro/index_dir/para_index有备份，但请勿修改

/root/test_code：建库过程中的各种测试代码

/root/index_write：建库的索引写入代码

/root/lucene/index_dir：挂载输出目录

======================可执行文件========================

node_index_search.py，python运行即可，用于查询节点（ABC类都可），输入为关键词

para_index_search.py，python运行即可，用于查询段落内容，输入为段落编号

show_node.py，python运行即可，用于展示节点规模，输入为每类节点展示的数量

text_index_search.py，python运行即可，用于展示文本与语义库的匹配度，输入为文本

========================最新======================**

/root/node_search_n.py 最新的节点查询程序，包含了格式转换

/root/para_index_search.py 段落查询程序

/root/json4show.py 生成首页展示json文件

==============================================**

维护：xh
