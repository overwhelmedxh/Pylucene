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

# 写入大概四分钟，总段落数3860000

def mkdirp(p):
    if not os.path.exists(p):
        os.makedirs(p, exist_ok=True)

def deletedir(p):
    if os.path.exists(p):
        shutil.rmtree(p, ignore_errors = True)

def index_docs(csv_path, index_path):

    file_list = os.listdir(csv_path)
    file_list.sort(key=lambda x:int(x[:4]))
    
    lucene.initVM()
    
    analyzer = StandardAnalyzer() 
    index_dir = store.FSDirectory.open(File(index_path).toPath()) 
    config = IndexWriterConfig(analyzer) 
    writer = IndexWriter(index_dir, config) 
    process_num = 0
    for file in file_list:
        
        file_path = os.path.join(csv_path, file)
        with open(file_path, 'r', encoding='utf-8') as f:
            # process_num = 0
            rows = csv.reader(f) 
            header = next(rows) # 过滤第一行
            for row in tqdm(rows):

                para_id = row[1] # d20121108001p001
                article_id = para_id[:-4]
                para_content = row[2]
                para_keyword = row[3]

                lucene_doc = Document() 
                # 编号不分词，需要索引，存储
                lucene_doc.add(StringField('para_id', para_id , Field.Store.YES))

                # 编号不分词，需要索引，存储
                lucene_doc.add(StringField('article_id', article_id , Field.Store.YES))

                # 内容不分词，不索引，但存储
                lucene_doc.add(StoredField('para_content', para_content))

                lucene_doc.add(StringField('para_keyword', para_keyword, Field.Store.YES))

                writer.addDocument(lucene_doc)

                process_num += 1
                if process_num % 20000 == 0:
                    print('已处理 '+str(process_num)+' 个段落\n')
            

    writer.commit()
    writer.close() 


if __name__ == '__main__':
    # 原始数据路径
    csv_path = '/root/data/paragraph/id_para_keywords'
    # 索引存放路径
    index_path = '/root/data/index_dir/content_index'

    deletedir(index_path) # 清空索引
    mkdirp(index_path)

    index_docs(csv_path, index_path)

    # 段落的索引文件共1017M
    print("\n索引写入完成\n")
