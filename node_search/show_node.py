import os
import json
import lucene
from java.io import File
from org.apache.lucene.search import IndexSearcher,TermQuery,PrefixQuery,WildcardQuery
from org.apache.lucene import store, queryparser
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.analysis.standard import StandardAnalyzer

# /root/data/index_dir/content_index

def search_node(parser, searcher):

    # =========根据编号查询（字符串==========

    query = parser.parse('a*') 
    hits = searcher.search(query, 10000) 
    number = hits.totalHits.value
    print('A类核心节点共' + str(number) + '个')
    a_nodes =[]

    for hit in hits.scoreDocs:
        doc_id = hit.doc # 这个ID是lucene分配的id
        doc =  searcher.doc(doc_id) # 读取文档本身
        score = hit.score # 命中分数
        a_nodes.append(doc.get("entity"))
        a_nodes.append(doc.get("node_id"))
    
    print(a_nodes)
        

if __name__ == '__main__':
    show_number = input('输入每类节点展示的数目（如200）：\n')
    index_path = '/root/node_index_dir'
    lucene.initVM()
    directory = store.FSDirectory.open(File(index_path).toPath()) 
    reader = DirectoryReader.open(directory) 
    numDocs = reader.numDocs()
    print("全部节点个数:", numDocs)

    searcher = IndexSearcher(reader) 
    analyzer = StandardAnalyzer() 
    parser = queryparser.classic.QueryParser('node_id', analyzer)

    # =========根据编号查询（字符串==========
    # query = PrefixQuery(Term("node_id", "a"))
    # query = WildcardQuery(Term("node_id", "a*"))
    query = parser.parse('a*') 
    hits = searcher.search(query, int(show_number)) 
    # 超过了默认的最大匹配文档数目（默认为1000），导致只返回了部分匹配的文档
    # number = hits.totalHits.value
    # print('A类核心节点共' + str(number) + '个')
    a_nodes =[]
    for hit in hits.scoreDocs:
        doc_id = hit.doc # 这个ID是lucene分配的id
        doc =  searcher.doc(doc_id) # 读取文档本身
        score = hit.score # 命中分数
        a_nodes.append(doc.get("entity") + '_' + doc.get("node_id"))
    print('\nA类节点99242个：')
    print(a_nodes)

    # =========根据编号查询（字符串==========
    # query = PrefixQuery(Term("node_id", "a"))
    # query = WildcardQuery(Term("node_id", "a*"))
    query = parser.parse('b*') 
    hits = searcher.search(query, int(show_number)) 
    # 超过了默认的最大匹配文档数目（默认为1000），导致只返回了部分匹配的文档
    # number = hits.totalHits.value
    # print('A类核心节点共' + str(number) + '个')
    a_nodes =[]
    for hit in hits.scoreDocs:
        doc_id = hit.doc # 这个ID是lucene分配的id
        doc =  searcher.doc(doc_id) # 读取文档本身
        score = hit.score # 命中分数
        a_nodes.append(doc.get("entity") + '_' + doc.get("node_id"))
    print('\nB类节点1000000个：')
    print(a_nodes)

    # =========根据编号查询（字符串==========
    # query = PrefixQuery(Term("node_id", "a"))
    # query = WildcardQuery(Term("node_id", "a*"))
    query = parser.parse('c*') 
    hits = searcher.search(query, int(show_number)) 
    # 超过了默认的最大匹配文档数目（默认为1000），导致只返回了部分匹配的文档
    # number = hits.totalHits.value
    # print('A类核心节点共' + str(number) + '个')
    a_nodes =[]
    for hit in hits.scoreDocs:
        doc_id = hit.doc # 这个ID是lucene分配的id
        doc =  searcher.doc(doc_id) # 读取文档本身
        score = hit.score # 命中分数
        a_nodes.append(doc.get("entity") + '_' + doc.get("node_id"))
    print('\nC类节点2498798：')
    print(a_nodes)

    reader.close()