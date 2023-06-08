import os
import json
import jieba
import lucene
from java.io import File
from org.apache.lucene.search import IndexSearcher,TermQuery
from org.apache.lucene import store, queryparser
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.analysis.standard import StandardAnalyzer
import logging
import jieba

jieba.setLogLevel(logging.WARNING)

# /root/data/index_dir/content_index

def search_node(entity, searcher):

    # =============根据关键词查询（中文字符串===========
    term = Term('entity',entity)
    query = TermQuery(term)
    hits = searcher.search(query, 1) 
    # print(hits.totalHits)
    if(hits.totalHits.value >= 1):
        for hit in hits.scoreDocs:
            doc_id = hit.doc # 这个ID是lucene分配的id
            doc =  searcher.doc(doc_id) # 读取文档本身
            return doc.get("entity"), doc.get("node_id")
    else:
        return 0, 'r'

def search_text(text, searcher):

    seg_list = jieba.cut(text, cut_all=True)
    seg_list = list(set(seg_list))

    term_number = 0
    hits_number = 0
    a_word_list = []
    b_word_list = []
    c_word_list = []
    for seg in seg_list:
        # word_list.append(seg)
        term_number += 1
        entity, node_id = search_node(seg, searcher)
        if(node_id[0] == 'a'):
            hits_number += 1
            a_word_list.append(entity + '_' + node_id)
        if(node_id[0] == 'b'):
            hits_number += 1
            b_word_list.append(entity + '_' + node_id)
        if(node_id[0] == 'c'):
            hits_number += 1
            c_word_list.append(entity + '_' + node_id)

    # a_word_list = list(set(a_word_list))
    # b_word_list = list(set(b_word_list))
    # c_word_list = list(set(c_word_list))
    print('\n命中A类（总数99242）核心词', len(a_word_list),'个：')
    if len(a_word_list) > 10 :
        print(a_word_list[:min(10, len(a_word_list))], " ...")
    else :
        print(a_word_list)

    print('\n命中B类（总数100000）次核心词', len(b_word_list),'个：')
    if len(b_word_list) > 10 :
        print(b_word_list[:min(10, len(b_word_list))], " ...")
    else :
        print(b_word_list)

    print('\n命中C类（总数2498799）外围词', len(c_word_list),'个：')
    if len(c_word_list) > 10 :
        print(c_word_list[:min(10, len(c_word_list))], " ...")
    else :
        print(c_word_list)

    if float(term_number) == 0.:
        similarity = 0
    else:
        similarity = float(hits_number)/float(term_number)
    similarity = similarity ** 0.3
    print('\n匹配度：', str(similarity))
    print('-'*40)

def search_text_2(text, searcher, num):

    seg_list = jieba.cut(text, cut_all=True)
    seg_list = list(set(seg_list))
    term_number = 0
    hits_number = 0
    a_word_list = []
    b_word_list = []
    c_word_list = []
    for seg in seg_list:
        term_number += 1
        entity, node_id = search_node(seg, searcher)
        if(node_id[0] == 'a'):
            hits_number += 1
            a_word_list.append(entity + '_' + node_id)
        if(node_id[0] == 'b'):
            hits_number += 1
            b_word_list.append(entity + '_' + node_id)
        if(node_id[0] == 'c'):
            hits_number += 1
            c_word_list.append(entity + '_' + node_id)

    print('\n命中A类（总数99242）核心词', len(a_word_list),'个：')
    if len(a_word_list) > 10 :
        print(a_word_list[:min(10, len(a_word_list))], " ...")
    else :
        print(a_word_list)

    print('\n命中B类（总数100000）次核心词', len(b_word_list),'个：')
    if len(b_word_list) > 10 :
        print(b_word_list[:min(10, len(b_word_list))], " ...")
    else :
        print(b_word_list)

    print('\n命中C类（总数2498799）外围词', len(c_word_list),'个：')
    if len(c_word_list) > 10 :
        print(c_word_list[:min(10, len(c_word_list))], " ...")
    else :
        print(c_word_list)

    if float(term_number) == 0.:
        similarity = 0
    else:
        similarity = float(hits_number)/float(term_number)
    similarity = similarity ** 0.3
    print('\n匹配度：', str(similarity))
    print('-'*40)

    text_dict = {
        "class_A":{
        "total_core_word": 99242,
        "hit_core_word": len(a_word_list),
        "word_list": a_word_list[:min(num, len(a_word_list))]
        },
        "class_B":{
        "total_core_word": 1000000,
        "hit_core_word": len(b_word_list),
        "word_list": b_word_list[:min(num, len(b_word_list))]
        },
        "class_C":{
        "total_core_word": 2498799,
        "hit_core_word": len(c_word_list),
        "word_list": c_word_list[:min(num, len(c_word_list))]
        },
        "similarity":str(similarity),
    }
    return text_dict

def text_search(text,num):
    index_path = '/root/data/index_dir/node_index'
    file_name = '/root/data/ngram_filter_1109.txt'
    jieba.load_userdict(file_name)
    if lucene.getVMEnv()==None:
        lucene.initVM()
    # vm_env = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    # vm_env.attachCurrentThread()#绑定线程
    directory = store.FSDirectory.open(File(index_path).toPath()) 
    reader = DirectoryReader.open(directory) 
    searcher = IndexSearcher(reader)
    result_data = search_text_2(text, searcher,num)
    print(result_data)
    reader.close()
    # vm_env.attachCurrentThread()#绑定线程
    return result_data

if __name__ == '__main__':
    # index_path = '/root/data/index_dir/node_index'
    # file_name = '/root/data/ngram_filter_1109.txt'
    # jieba.load_userdict(file_name)
    # lucene.initVM()
    # directory = store.FSDirectory.open(File(index_path).toPath()) 
    # reader = DirectoryReader.open(directory) 
    # searcher = IndexSearcher(reader)

    # text = input('输入文本：')
    # print(text)
    # search_text(text, searcher)
    # reader.close()
    text_search("新华社",20)

