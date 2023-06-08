import os
import sys
import json
import csv
import lucene
from java.io import File
from datetime import datetime
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, StringField, TextField, StoredField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene import store
from org.apache.lucene.util import Version
import shutil
from tqdm import tqdm

# 写入约五分钟
# A类，共99242个节点
# B类，共1000000个节点
# C类，共2498798个节点

csv.field_size_limit(500 * 1024 * 1024)

def mkdirp(p):
    if not os.path.exists(p):
        os.makedirs(p, exist_ok=True)

def deletedir(p):
    if os.path.exists(p):
        shutil.rmtree(p, ignore_errors = True)

def index_docs(a_csv_path, b_csv_path, c_csv_path, index_path):
    lucene.initVM()
    analyzer = StandardAnalyzer() 
    index_dir = store.FSDirectory.open(File(index_path).toPath()) 
    config = IndexWriterConfig(analyzer) 
    index_writer = IndexWriter(index_dir, config)

    process_num = 0
    with open(a_csv_path, 'r', encoding='utf-8') as f: 
        rows = csv.reader(f) 
        # header = next(rows) # 过滤第一行
        for row in tqdm(rows):

            node_id = row[0]         # 节点编号
            node_id = 'a' + node_id[1:]
            entity = row[1]          # 实体
            word_fre = row[2]        # 词频
            word_label = row[3]      # 词性
            entity_type = row[4]     # 实体类型
            source = row[5]          # 来源
            vector = row[7]          # 向量表示

            neighbor = '_'.join([row[8], row[9], row[10]])
            neighbor = neighbor.strip('null').replace('_null','').strip('_')
            sentiment = '积极'       # 主流情感

            lucene_doc = Document() 
            # 编号不分词，需要索引，存储
            lucene_doc.add(StringField('node_id', node_id , Field.Store.YES))
            # 实体（关键词）不分词，需要索引，存储
            lucene_doc.add(StringField('entity', entity , Field.Store.YES))
            # 词频不分词，不索引，但存储
            lucene_doc.add(StoredField('word_fre', word_fre))
            # 词性不分词，不索引，但存储
            lucene_doc.add(StoredField('word_label', word_label))
            # 实体类型不分词，不索引，但存储
            lucene_doc.add(StoredField('entity_type', entity_type))
            # 来源不分词，不索引，但存储
            lucene_doc.add(StoredField('source', source))
            # 向量不分词，不索引，但存储
            lucene_doc.add(StoredField('vector', vector))
            # 邻节点不分词，不索引，但存储
            lucene_doc.add(StoredField('neighbor', neighbor))
            # 主流情感不分词，不索引，但存储
            lucene_doc.add(StoredField('sentiment', sentiment))
            

            index_writer.addDocument(lucene_doc)

            process_num += 1
            # if process_num % 2000 == 0:
            #     print('已处理 '+str(process_num)+' 个节点\n')
    print('一共 '+str(process_num)+' 个A核心节点\n')        
    index_writer.commit()

    process_num = 0
    with open(b_csv_path, 'r', encoding='utf-8') as f: 
        rows = csv.reader(f) 
        # header = next(rows) # 过滤第一行
        for row in tqdm(rows):

            node_id = row[0]         # 节点编号
            node_id = 'b' + node_id[1:]
            entity = row[1]          # 实体
            word_fre = '待补充'        # 词频
            word_label = row[2]      # 词性
            entity_type = row[3]      # 实体类型
            source = '待补充'          # 来源
            vector = '待补充'           # 向量表示

            neighbor = '_'.join([row[4], row[5], row[6]])
            neighbor = neighbor.strip('null').replace('_null','').strip('_')

            sentiment = '积极'       # 主流情感

            lucene_doc = Document() 
            # 编号不分词，需要索引，存储
            lucene_doc.add(StringField('node_id', node_id , Field.Store.YES))
            # 实体（关键词）不分词，需要索引，存储
            lucene_doc.add(StringField('entity', entity , Field.Store.YES))
            # 词频不分词，不索引，但存储
            lucene_doc.add(StoredField('word_fre', word_fre))
            # 词性不分词，不索引，但存储
            lucene_doc.add(StoredField('word_label', word_label))
            # 实体类型不分词，不索引，但存储
            lucene_doc.add(StoredField('entity_type', entity_type))
            # 来源不分词，不索引，但存储
            lucene_doc.add(StoredField('source', source))
            # 向量不分词，不索引，但存储
            lucene_doc.add(StoredField('vector', vector))
            # 邻节点不分词，不索引，但存储
            lucene_doc.add(StoredField('neighbor', neighbor))
            # 主流情感不分词，不索引，但存储
            lucene_doc.add(StoredField('sentiment', sentiment))
            index_writer.addDocument(lucene_doc)

            process_num += 1
            # if process_num % 2000 == 0:
            #     print('已处理 '+str(process_num)+' 个节点\n')
    print('一共 '+str(process_num)+' 个B核心节点\n')  

    process_num = 0
    with open(c_csv_path, 'r', encoding='utf-8') as f: 
        rows = csv.reader(f) 
        # header = next(rows) # 过滤第一行
        for row in tqdm(rows):

            node_id = row[0]         # 节点编号
            node_id = 'c' + node_id[1:]
            entity = row[1]          # 实体
            word_fre = '待补充'        # 词频
            word_label = row[2]      # 词性
            entity_type = row[3]      # 实体类型
            source = '待补充'          # 来源
            vector = '待补充'           # 向量表示

            neighbor = '_'.join([row[4], row[5], row[6]])
            neighbor = neighbor.strip('null').replace('_null','').strip('_')
            
            sentiment = '中性'       # 主流情感

            lucene_doc = Document() 
            # 编号不分词，需要索引，存储
            lucene_doc.add(StringField('node_id', node_id , Field.Store.YES))
            # 实体（关键词）不分词，需要索引，存储
            lucene_doc.add(StringField('entity', entity , Field.Store.YES))
            # 词频不分词，不索引，但存储
            lucene_doc.add(StoredField('word_fre', word_fre))
            # 词性不分词，不索引，但存储
            lucene_doc.add(StoredField('word_label', word_label))
            # 实体类型不分词，不索引，但存储
            lucene_doc.add(StoredField('entity_type', entity_type))
            # 来源不分词，不索引，但存储
            lucene_doc.add(StoredField('source', source))
            # 向量不分词，不索引，但存储
            lucene_doc.add(StoredField('vector', vector))
            # 邻节点不分词，不索引，但存储
            lucene_doc.add(StoredField('neighbor', neighbor))
            # 主流情感不分词，不索引，但存储
            lucene_doc.add(StoredField('sentiment', sentiment))
            index_writer.addDocument(lucene_doc)

            process_num += 1
            # if process_num % 2000 == 0:
            #     print('已处理 '+str(process_num)+' 个节点\n')
    print('一共 '+str(process_num)+' 个C核心节点\n') 

    
    index_writer.commit()
    index_writer.close() 


if __name__ == '__main__':
    # 原始数据路径
    a_csv_path = '/root/data/script/final/result_10w_v4.1.csv'
    b_csv_path = '/root/data/script/script_results/100w/result_100w_v1.csv'
    c_csv_path = '/root/data/script/script_results/250w/result_250w_v1.csv'
    # 索引存放路径
    index_path = '/root/node_index_dir'

    deletedir(index_path) # 清空索引
    mkdirp(index_path)

    index_docs(a_csv_path, b_csv_path, c_csv_path, index_path)

    print("\n索引写入完成\n")
