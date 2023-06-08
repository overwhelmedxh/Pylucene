import os
import json
import lucene
from java.io import File
from org.apache.lucene.search import IndexSearcher,TermQuery
from org.apache.lucene import store, queryparser
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.analysis.standard import StandardAnalyzer
import time
# /root/data/index_dir/content_index

def search_node(entity, searcher):
    # =========根据编号查询（字符串==========
    # analyzer = StandardAnalyzer() 
    # parser = queryparser.classic.QueryParser('node_id', analyzer) 
    # query = parser.parse('a00001') 

    # 全面深化改革
    # 人类命运共同体
    # 党组织
    # 贫困地区
    # 作风从严 b节点
    # 长江干线水上交通安全管理特别规定 c节点
    # 夏厚 没有这个对应的节点

    # =============根据关键词查询（中文字符串===========
    term = Term('entity',entity)
    query = TermQuery(term)

    hits = searcher.search(query, 1) 
    
    # print(hits.totalHits)
    if hits.totalHits.value == 0 :
        print('没有这个节点!\n')
        return {}

    for hit in hits.scoreDocs:
        doc_id = hit.doc # 这个ID是lucene分配的id
        doc =  searcher.doc(doc_id) # 读取文档本身

        source = doc.get("source").split('_')
        vector = doc.get("vector").split('_')
        neighbor = doc.get("neighbor").split('_')
        # print(len(neighbor))
        resultss = {}

        if doc.get("node_id")[0] == 'a':
            neighbors = []
            neighbors_a = []
            neighbors_b = []
            neighbors_c = []
            for neig in neighbor:
                term_1 = Term('entity',neig)
                query_1 = TermQuery(term_1)
                results = searcher.search(query_1, 1)
                for result in results.scoreDocs:
                    doc_id_1 = result.doc 
                    doc_1 =  searcher.doc(doc_id_1)
                    neigbor_id =  doc_1.get("node_id")
                    # neighbors.append(neig + '_' + neigbor_id)
                    if neigbor_id[0] == 'a':
                        neighbors_a.append(neig + '_' + neigbor_id) 
                    if neigbor_id[0] == 'b':
                        neighbors_b.append(neig + '_' + neigbor_id) 
                    if neigbor_id[0] == 'c':
                        neighbors_c.append(neig + '_' + neigbor_id)   
            # 这是得到的相邻节点，可以随机抽取一下    
            # sorted_neighbors = neighbors # random.sample(neighbors, node_n)  # sorted(neighbors, key=lambda x: x.split('_')[1][0])            
            resultss['关键词'] = doc.get("entity")
            resultss['节点编号'] = doc.get("node_id")
            resultss['相邻个数'] = len(neighbors_a) + len(neighbors_b) + len(neighbors_c)
            resultss['A类相邻节点'] = neighbors_a
            resultss['B类相邻节点'] = neighbors_b
            resultss['C类相邻节点'] = neighbors_c

        if doc.get("node_id")[0] == 'b':
            neighbors = [] 
            neighbors_a = []
            neighbors_b = []
            neighbors_c = []
            for neig in neighbor:
                term_1 = Term('entity',neig)
                query_1 = TermQuery(term_1)
                results = searcher.search(query_1, 1)
                for result in results.scoreDocs:
                    doc_id_1 = result.doc 
                    doc_1 =  searcher.doc(doc_id_1)
                    neigbor_id =  doc_1.get("node_id")
                    # neighbors.append(neig + '_' + neigbor_id)  
                    if neigbor_id[0] == 'a':
                        neighbors_a.append(neig + '_' + neigbor_id) 
                    if neigbor_id[0] == 'b':
                        neighbors_b.append(neig + '_' + neigbor_id) 
                    if neigbor_id[0] == 'c':
                        neighbors_c.append(neig + '_' + neigbor_id)     
            sorted_neighbors = neighbors

            resultss['关键词'] = doc.get("entity")
            resultss['节点编号'] = doc.get("node_id")
            resultss['相邻个数'] = len(neighbors_a) + len(neighbors_b) + len(neighbors_c)
            resultss['A类相邻节点'] = neighbors_a
            resultss['B类相邻节点'] = neighbors_b
            resultss['C类相邻节点'] = neighbors_c

        if doc.get("node_id")[0] == 'c':
            neighbors = [] 
            neighbors_a = []
            neighbors_b = []
            neighbors_c = []
            for neig in neighbor:
                term_1 = Term('entity',neig)
                query_1 = TermQuery(term_1)
                results = searcher.search(query_1, 1)
                for result in results.scoreDocs:
                    doc_id_1 = result.doc 
                    doc_1 =  searcher.doc(doc_id_1)
                    neigbor_id =  doc_1.get("node_id")
                    # neighbors.append(neig + '_' + neigbor_id) 
                    if neigbor_id[0] == 'a':
                        neighbors_a.append(neig + '_' + neigbor_id) 
                    if neigbor_id[0] == 'b':
                        neighbors_b.append(neig + '_' + neigbor_id) 
                    if neigbor_id[0] == 'c':
                        neighbors_c.append(neig + '_' + neigbor_id)       
            sorted_neighbors = neighbors

            resultss['关键词'] = doc.get("entity")
            resultss['节点编号'] = doc.get("node_id")
            resultss['相邻个数'] = len(neighbors_a) + len(neighbors_b) + len(neighbors_c)
            resultss['A类相邻节点'] = neighbors_a
            resultss['B类相邻节点'] = neighbors_b
            resultss['C类相邻节点'] = neighbors_c

        print('-'*40)
        return resultss


if __name__ == '__main__':

    index_path = '/root/node_index_dir'
    lucene.initVM()
    directory = store.FSDirectory.open(File(index_path).toPath()) 
    reader = DirectoryReader.open(directory) 
    searcher = IndexSearcher(reader) 

    print('-'*40)
    entity = input('请输入关键词（如人类命运共同体）：\n')
    result = search_node(entity, searcher)
    print('关键词：'+ result['关键词'])
    print('节点编号：'+ result['节点编号'])
    print('相邻个数：'+ str(result['相邻个数']))
    print('A类相邻节点：')
    print(result['A类相邻节点'])
    print('B类相邻节点：')
    print(result['B类相邻节点'])
    print('C类相邻节点：')
    print(result['C类相邻节点'])

    while(1):
        entity = input('是否继续检索（是请继续输入关键词，否请输入exit）：')
        if(entity == 'exit'):
            break
        else:
            result = search_node(entity, searcher)
            print('关键词：'+ result['关键词'])
            print('节点编号：'+ result['节点编号'])
            print('相邻个数：'+ str(result['相邻个数']))
            print('A类相邻节点：')
            print(result['A类相邻节点'])
            print('B类相邻节点：')
            print(result['B类相邻节点'])
            print('C类相邻节点：')
            print(result['C类相邻节点'])


        # print(result)

    reader.close()
