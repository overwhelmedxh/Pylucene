import os
import json
import lucene
from java.io import File
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene import store, queryparser
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.analysis.standard import StandardAnalyzer

def id2date(para_id): # d201212304752p010
    date = para_id[1:5] + '年' + para_id[5:7] + '月' + para_id[7:9] + '日'
    aticle_num = para_id[9:12]
    para_num = para_id[-3:]
    return date, aticle_num, para_num

def para2artice(para_id, parser, searcher):
    a_id = para_id[:-4] # d20121122996
    query = parser.parse('article_id:'+a_id) 
    hits = searcher.search(query, 99).scoreDocs

    a_content = ''
    for hit in hits:
        doc_id = hit.doc # 这个ID是lucene分配的id
        doc =  searcher.doc(doc_id) # 读取文档本身
        score = hit.score # 命中分数
        para_id = doc.get("para_id")

        a_content += doc.get("para_content")
        
    # print("\n文章内容为：", a_content, '\n') # 获取field内容，输出
    return a_content

def search_para(para_id, parser, searcher):

    # d20220419028p004
    # d20181015053p029
    # d20140716048p025
    # d20211110041p008
    # d20160101001p019 没有这个编号对应的内容
    query = parser.parse(para_id) 

    hits = searcher.search(query, 1) 
    para_data = {
        "location":"没有这个段落!\n",
        "keyword":[],
        "para_content":"",
        "text_content":""
    }
    # print(hits.totalHits)
    if hits.totalHits.value == 0 :
        return para_data

    for hit in hits.scoreDocs:
        doc_id = hit.doc # 这个ID是lucene分配的id
        doc =  searcher.doc(doc_id) # 读取文档本身
        score = hit.score # 命中分数
        para_id = doc.get("para_id")
        key_word = doc.get("para_keyword").split('_')

        date, a_num, p_num = id2date(para_id)
        # print("\n这是" + date + '的第' + a_num + '篇文章的第' + p_num + '段')
        # print("关键词为：", key_word)
        # print("\n段落内容为：", doc.get("para_content")) # 获取field内容，输出

        # 根据段落id，检索并输出全文
        text_content = para2artice(para_id, parser, searcher)
        para_data["location"] = str("\n这是" + date + '的第' + a_num + '篇文章的第' + p_num + '段')
        para_data["keyword"] = key_word
        para_data["para_content"] = doc.get("para_content")
        para_data["text_content"] = text_content

    return para_data

def para_search(para_id:str):
    index_path = '/root/data/index_dir/content_index'
    if lucene.getVMEnv()==None:
        lucene.initVM()
    # print("env------------")
    # print(lucene.getVMEnv())
    directory = store.FSDirectory.open(File(index_path).toPath()) 
    reader = DirectoryReader.open(directory) 
    searcher = IndexSearcher(reader) 
    analyzer = StandardAnalyzer() 
    parser = queryparser.classic.QueryParser('para_id', analyzer)

    result_data = search_para(para_id, parser, searcher)
    print(result_data)
    return result_data

 

if __name__ == '__main__':
    para_search("d20211110041p008")
    # index_path = '/root/data/index_dir/content_index'
    # lucene.initVM()
    # directory = store.FSDirectory.open(File(index_path).toPath()) 
    # reader = DirectoryReader.open(directory) 
    # searcher = IndexSearcher(reader) 
    # analyzer = StandardAnalyzer() 
    # parser = queryparser.classic.QueryParser('para_id', analyzer)

    # para_id = input('输入段落编号（形如d20211110041p008）：\n')
    # result_data = search_para(para_id, parser, searcher)
    # print(result_data)
    
    # reader.close()
    