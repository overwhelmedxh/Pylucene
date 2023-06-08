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

    for hit in hits.scoreDocs:
        doc_id = hit.doc # 这个ID是lucene分配的id
        doc =  searcher.doc(doc_id) # 读取文档本身

        source = doc.get("source").split('_')
        vector = doc.get("vector").split('_')
        neighbor = doc.get("neighbor").split('_')
        # print(len(neighbor))

        if doc.get("node_id")[0] == 'a':
            neighbors = [] 
            for neig in neighbor:
                term_1 = Term('entity',neig)
                query_1 = TermQuery(term_1)
                results = searcher.search(query_1, 1)
                for result in results.scoreDocs:
                    doc_id_1 = result.doc 
                    doc_1 =  searcher.doc(doc_id_1)
                    neigbor_id =  doc_1.get("node_id")
                    neighbors.append(neig + '_' + neigbor_id)       
            sorted_neighbors = neighbors # sorted(neighbors, key=lambda x: x.split('_')[1][0])            

            print("关键词：" + doc.get("entity"))
            print("节点编号：" + doc.get("node_id"))
            print("词频：", doc.get("word_fre"))
            print("词性：", doc.get("word_label"))
            print("实体类型：", doc.get("entity_type"))
            if len(source) > 10 :
                print("来源（共%d个）：" % (len(source)), source[:min(10, len(source))], "...")
            else :
                print("来源（共%d个）：" % (len(source)), source[:min(10, len(source))])
            #print("\n向量表征(300维）：", vector)
            if len(sorted_neighbors) > 10 :
                print("相邻节点（共%d个）：" % len(sorted_neighbors), sorted_neighbors[:min(10, len(sorted_neighbors))], " ...")
            else :
                print("相邻节点（共%d个）：" % len(sorted_neighbors), sorted_neighbors[:min(10, len(sorted_neighbors))])
            print("主流情感：", doc.get("sentiment"))

        if doc.get("node_id")[0] == 'b':
            neighbors = [] 
            for neig in neighbor:
                term_1 = Term('entity',neig)
                query_1 = TermQuery(term_1)
                results = searcher.search(query_1, 1)
                for result in results.scoreDocs:
                    doc_id_1 = result.doc 
                    doc_1 =  searcher.doc(doc_id_1)
                    neigbor_id =  doc_1.get("node_id")
                    neighbors.append(neig + '_' + neigbor_id)       
            sorted_neighbors = neighbors

            print("关键词：" + doc.get("entity"))
            print("节点编号：" + doc.get("node_id"))
            print("词性：", doc.get("word_label"))
            print("实体类型：", doc.get("entity_type"))
            if len(sorted_neighbors) > 10 :
                print("相邻节点（共%d个）：" % len(sorted_neighbors), sorted_neighbors[:min(10, len(sorted_neighbors))], " ...")
            else :
                print("相邻节点（共%d个）：" % len(sorted_neighbors), sorted_neighbors[:min(10, len(sorted_neighbors))])
            print("主流情感：", doc.get("sentiment"))

        if doc.get("node_id")[0] == 'c':
            neighbors = [] 
            for neig in neighbor:
                term_1 = Term('entity',neig)
                query_1 = TermQuery(term_1)
                results = searcher.search(query_1, 1)
                for result in results.scoreDocs:
                    doc_id_1 = result.doc 
                    doc_1 =  searcher.doc(doc_id_1)
                    neigbor_id =  doc_1.get("node_id")
                    neighbors.append(neig + '_' + neigbor_id)       
            sorted_neighbors = neighbors

            print("关键词：" + doc.get("entity"))
            print("节点编号：" + doc.get("node_id"))
            print("词性：", doc.get("word_label"))
            print("实体类型：", doc.get("entity_type"))
            if len(sorted_neighbors) > 10 :
                print("相邻节点（共%d个）：" % len(sorted_neighbors), sorted_neighbors[:min(10, len(sorted_neighbors))], " ...")
            else :
                print("相邻节点（共%d个）：" % len(sorted_neighbors), sorted_neighbors[:min(10, len(sorted_neighbors))])
            print("主流情感：", doc.get("sentiment"))

        print('-'*40)


def show_node(searcher):
    # show_number = 10
    # analyzer = StandardAnalyzer() 
    # parser = queryparser.classic.QueryParser('node_id', analyzer)

    # query = parser.parse('a*') 
    # hits = searcher.search(query, show_number) 
    # a_nodes =[]
    # for hit in hits.scoreDocs:
    #     doc_id = hit.doc # 这个ID是lucene分配的id
    #     doc =  searcher.doc(doc_id) # 读取文档本身
    #     score = hit.score # 命中分数
    #     a_nodes.append(doc.get("entity") + '_' + doc.get("node_id"))
    a_nodes = ['中国_a00002','教育_a00029','思想_a00060','战略_a00067','文化交流合作_a13187','平凡岗位_a13315','打击非法_a13467','深圳市政府_a28636','金融产业_a39296','中缅友好_a39472']
    print('包含核心节点99242个：')
    print(a_nodes, " ...")
    time.sleep(2)

    # query = parser.parse('b*') 
    # hits = searcher.search(query, int(show_number)) 
    # a_nodes =[]
    # for hit in hits.scoreDocs:
    #     doc_id = hit.doc # 这个ID是lucene分配的id
    #     doc =  searcher.doc(doc_id) # 读取文档本身
    #     score = hit.score # 命中分数
    #     a_nodes.append(doc.get("entity") + '_' + doc.get("node_id"))
    b_nodes = ['作风从严_b0000010','政治委员制_b0000020','远程防空导弹_b0042657','山东省蓬莱市_b0100056','一汽大众捷达_b0100150','小微企业安全_b0100301','统招硕士_b0238470','交通安全知识课_b0389732','人民群众的安全感_b0500011','物联网大会_b0817669']
    print('次核心节点1000000个：')
    print(b_nodes, " ...")
    time.sleep(2)

    # query = parser.parse('c*') 
    # hits = searcher.search(query, int(show_number)) 
    # a_nodes =[]
    # for hit in hits.scoreDocs:
    #     doc_id = hit.doc # 这个ID是lucene分配的id
    #     doc =  searcher.doc(doc_id) # 读取文档本身
    #     score = hit.score # 命中分数
    #     a_nodes.append(doc.get("entity") + '_' + doc.get("node_id"))
    c_nodes = ['创意时代_c0000062','发展质量明显提升_c0000129','教育的振兴_c0128646','摸索中前进_c0275633','市场期待_c0470520','腾讯智慧峰会_c0676286','高速公路企业_c1058503','重庆德庄火锅_c1495744','人口增长速度_c1705117','大数据服务中心_c2072564']
    print('外围节点2498798个：')
    print(c_nodes, " ...")
    time.sleep(2)

if __name__ == '__main__':


    print('主流价值观标签语义知识体系系统正在加载···')
    time.sleep(3)
    index_path = '/root/node_index_dir'
    lucene.initVM()
    directory = store.FSDirectory.open(File(index_path).toPath()) 
    reader = DirectoryReader.open(directory) 
    searcher = IndexSearcher(reader) 

    print('主流价值观标签语义知识体系系统已加载完毕！')
    time.sleep(1)
    show_node(searcher)
    print('-'*40)
    entity = input('请输入关键词（如人类命运共同体）：\n')
    search_node(entity, searcher)

    while(1):
        #entity = input('输入关键词（如人类命运共同体）：\n')
        #search_node(entity, searcher)
        entity = input('是否继续检索（是请继续输入关键词，否请输入exit）：')
        if(entity == 'exit'):
            break
        else:
            search_node(entity, searcher)

    reader.close()
