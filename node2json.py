import os
import json
import lucene
import random
from java.io import File
from org.apache.lucene.search import IndexSearcher,TermQuery
from org.apache.lucene import store, queryparser
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.analysis.standard import StandardAnalyzer
import time
# /root/data/index_dir/content_index

def search_node(entity, searcher, node_n):
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
            for neig in neighbor:
                term_1 = Term('entity',neig)
                query_1 = TermQuery(term_1)
                results = searcher.search(query_1, 1)
                for result in results.scoreDocs:
                    doc_id_1 = result.doc 
                    doc_1 =  searcher.doc(doc_id_1)
                    neigbor_id =  doc_1.get("node_id")
                    neighbors.append(neig + '_' + neigbor_id)   
            # 这是得到的相邻节点，可以随机抽取一下    
            sorted_neighbors = neighbors # random.sample(neighbors, node_n)  # sorted(neighbors, key=lambda x: x.split('_')[1][0])            

            resultss['关键词'] = doc.get("entity")
            resultss['节点编号'] = doc.get("node_id")
            resultss['词频'] = doc.get("word_fre")
            resultss['词性'] = doc.get("word_label")
            resultss['实体类型'] = doc.get("entity_type")
            resultss['来源'] = source[:min(node_n, len(source))]
            resultss['来源长度'] = len(source)
            resultss['相邻节点'] = sorted_neighbors[:min(node_n, len(sorted_neighbors))]
            resultss['相邻个数'] = len(sorted_neighbors)
            resultss['主流情感'] = doc.get("sentiment")

            print("关键词：" + doc.get("entity"))
            print("节点编号：" + doc.get("node_id"))
            print("词频：", doc.get("word_fre"))
            print("词性：", doc.get("word_label"))
            print("实体类型：", doc.get("entity_type"))
            if len(source) > node_n :
                print("来源（共%d个）：" % (len(source)), source[:min(node_n, len(source))], "...")
            else :
                print("来源（共%d个）：" % (len(source)), source[:min(node_n, len(source))])
            #print("\n向量表征(300维）：", vector)
            if len(sorted_neighbors) > node_n :
                print("相邻节点（共%d个）：" % len(sorted_neighbors), sorted_neighbors[:min(node_n, len(sorted_neighbors))], " ...")
            else :
                print("相邻节点（共%d个）：" % len(sorted_neighbors), sorted_neighbors[:min(node_n, len(sorted_neighbors))])
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

            resultss['关键词'] = doc.get("entity")
            resultss['节点编号'] = doc.get("node_id")
            # result['词频'] = doc.get("word_fre")
            resultss['词性'] = doc.get("word_label")
            resultss['实体类型'] = doc.get("entity_type")
            # result['来源'] = source[:min(node_n, len(source))]
            # result['来源长度'] = len(source)
            resultss['相邻节点'] = sorted_neighbors[:min(node_n, len(sorted_neighbors))]
            resultss['相邻个数'] = len(sorted_neighbors)
            resultss['主流情感'] = doc.get("sentiment")

            print("关键词：" + doc.get("entity"))
            print("节点编号：" + doc.get("node_id"))
            print("词性：", doc.get("word_label"))
            print("实体类型：", doc.get("entity_type"))
            if len(sorted_neighbors) > node_n :
                print("相邻节点（共%d个）：" % len(sorted_neighbors), sorted_neighbors[:min(node_n, len(sorted_neighbors))], " ...")
            else :
                print("相邻节点（共%d个）：" % len(sorted_neighbors), sorted_neighbors[:min(node_n, len(sorted_neighbors))])
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

            resultss['关键词'] = doc.get("entity")
            resultss['节点编号'] = doc.get("node_id")
            # result['词频'] = doc.get("word_fre")
            resultss['词性'] = doc.get("word_label")
            resultss['实体类型'] = doc.get("entity_type")
            # result['来源'] = source[:min(node_n, len(source))]
            # result['来源长度'] = len(source)
            resultss['相邻节点'] = sorted_neighbors[:min(node_n, len(sorted_neighbors))]
            resultss['相邻个数'] = len(sorted_neighbors)
            resultss['主流情感'] = doc.get("sentiment")

            print("关键词：" + doc.get("entity"))
            print("节点编号：" + doc.get("node_id"))
            print("词性：", doc.get("word_label"))
            print("实体类型：", doc.get("entity_type"))
            if len(sorted_neighbors) > node_n :
                print("相邻节点（共%d个）：" % len(sorted_neighbors), sorted_neighbors[:min(node_n, len(sorted_neighbors))], " ...")
            else :
                print("相邻节点（共%d个）：" % len(sorted_neighbors), sorted_neighbors[:min(node_n, len(sorted_neighbors))])
            print("主流情感：", doc.get("sentiment"))

        print('-'*40)
        return resultss

def show_node(searcher):

    a_nodes = ['中国_a00002','教育_a00029','思想_a00060','战略_a00067','文化交流合作_a13187','平凡岗位_a13315','打击非法_a13467','深圳市政府_a28636','金融产业_a39296','中缅友好_a39472']
    print('包含核心节点99242个：')
    print(a_nodes, " ...")
    time.sleep(2)

    b_nodes = ['作风从严_b0000010','政治委员制_b0000020','远程防空导弹_b0042657','山东省蓬莱市_b0100056','一汽大众捷达_b0100150','小微企业安全_b0100301','统招硕士_b0238470','交通安全知识课_b0389732','人民群众的安全感_b0500011','物联网大会_b0817669']
    print('次核心节点1000000个：')
    print(b_nodes, " ...")
    time.sleep(2)

    c_nodes = ['创意时代_c0000062','发展质量明显提升_c0000129','教育的振兴_c0128646','摸索中前进_c0275633','市场期待_c0470520','腾讯智慧峰会_c0676286','高速公路企业_c1058503','重庆德庄火锅_c1495744','人口增长速度_c1705117','大数据服务中心_c2072564']
    print('外围节点2498798个：')
    print(c_nodes, " ...")
    time.sleep(2)


def transform_node(input_dict, num_nodes):
    
    node_info = {}
    sorted_neighbors_num = input_dict['相邻个数']
    sorted_neighbors = input_dict['相邻节点']
    if sorted_neighbors_num>num_nodes:
        sorted_neighbors.append("...")    
    node_info.update({
        "keyword": input_dict['关键词'], 
        "node_id": input_dict['节点编号'],
        "word_fre": "",
        "word_label": input_dict['词性'],
        "entity_type": input_dict['实体类型'],
        "source": [],
        "source_len": 0,
        "sorted_neighbors": sorted_neighbors,
        "sorted_neighbors_num": sorted_neighbors_num,
        "sentiment": input_dict['主流情感']
    })
    
    
    source = input_dict['来源']
    source_len = input_dict['来源长度']
    if source_len>num_nodes:
        source.append("...")
    node_info.update({
        "word_fre": input_dict['词频'],
        "source": source,
        "source_len": source_len
    })
          
    # with open('/root/3node.json', 'w', encoding='utf-8') as fw:
    #     json.dump(node_info,  fw, indent=4, ensure_ascii=False)
    return node_info


if __name__ == '__main__':

    index_path = '/root/node_index_dir'
    lucene.initVM()
    directory = store.FSDirectory.open(File(index_path).toPath()) 
    reader = DirectoryReader.open(directory) 
    searcher = IndexSearcher(reader) 

    print('-'*40)
    entity1 = '人类命运共同体' # input('请输入关键词（如人类命运共同体）：\n')
    result1 = search_node(entity1, searcher, 50) # 20为需要返回的邻节点个数，可调节
    result1 = transform_node(result1, 50)

    entity2 = '全面深化改革' 
    result2 = search_node(entity2, searcher, 50) 
    result2 = transform_node(result2, 50)

    entity3 = '全国人大' 
    result3 = search_node(entity3, searcher, 50) 
    result3 = transform_node(result3, 50)

    result = [result1, result2, result3]
    with open('/root/3node.json', 'w', encoding='utf-8') as fw:
        json.dump(result,  fw, indent=4, ensure_ascii=False)

    reader.close()