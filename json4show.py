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
            # sorted_neighbors = neighbors # random.sample(neighbors, node_n)  # sorted(neighbors, key=lambda x: x.split('_')[1][0])            
            resultss['关键词'] = entity
            resultss['相邻个数'] = len(neighbors)
            resultss['相邻节点'] = neighbors

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

            resultss['关键词'] = entity
            resultss['相邻个数'] = len(neighbors)
            resultss['相邻节点'] = neighbors

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

            resultss['关键词'] = entity
            resultss['相邻个数'] = len(neighbors)
            resultss['相邻节点'] = neighbors

        # print('-'*40)
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

##################################################
'''
# gaojc 后处理代码
Input args:
    input_dict: 系统输出的原始字典
    num_nodes: 显示的相关节点数量, 随着系统输出个数更改
out args:
    result: 前端需要的json格式
'''
def transform(input_dict, num_nodes):
    output_dict = {
                    "categories": [
                    {
                        "name": "核心节点"
                    },
                    {
                        "name": "次核心节点"
                    },
                    {
                        "name": "外围节点"
                    }
                    ],
                    "nodes": [], "links": []}
    num_nodes += 1

    for i in range(num_nodes):
        node_dict = {}
        
        if i == 0:
            other_nodes = []
            node_dict.update({"id": i, "name": input_dict['关键词']})
            input_dict.pop("关键词")
            node_dict.update(input_dict)
            
            if input_dict["节点编号"][0] == 'a':
                node_dict.update({"category": 0})
            elif input_dict["节点编号"][0] == 'b':
                node_dict.update({"category": 1})
            else:
                node_dict.update({"category": 2})
            for key in input_dict.keys():
                if "相邻节点" in key:
                    other_nodes = input_dict[key]
        
        else:
            name = other_nodes[i-1].split('_')[0]
            num = other_nodes[i-1].split('_')[1]
            node_dict.update({"id": i, "name": name})
            
            if num[0] == 'a':
                node_dict.update({"category": 0})
            elif num[0] == 'b':
                node_dict.update({"category": 1})
            else:
                node_dict.update({"category": 2})
            output_dict["links"].append({"source": "0", "target": str(i)})

        output_dict["nodes"].append(node_dict)

    with open('test.json', 'w', encoding='utf-8') as fw:
        json.dump(output_dict,  fw, indent=4, ensure_ascii=False)
    return output_dict

def transform_node(input_dict, num_nodes):
    output_dict = {
                    "categories": [
                    {
                        "name": "核心节点"
                    },
                    {
                        "name": "次核心节点"
                    },
                    {
                        "name": "外围节点"
                    }
                    ],
                    "nodes": [], "links": [],"node_info":{}}
    num_nodes += 1
    node_info = {}
    for i in range(num_nodes):
        node_dict = {}
        
        if i == 0:
            other_nodes = []
            node_dict.update({"id": i, "name": input_dict['关键词']})
            node_dict.update({"symbolSize": 48})
            # 这里重新构造
            # input_dict.pop("关键词")
            # node_dict.update(input_dict)
            #  print("主流情感：", input_dict['主流情感'])
            # node_info.update(input_dict)
            # 对里面的关键词英化
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
            
            if input_dict["节点编号"][0] == 'a':
                source = input_dict['来源']
                source_len = input_dict['来源长度']
                if source_len>num_nodes:
                    source.append("...")
                node_info.update({
                    "word_fre": input_dict['词频'],
                    "source": source,
                    "source_len": source_len
                })
                node_dict.update({"category": 0})
            elif input_dict["节点编号"][0] == 'b':
                node_dict.update({"category": 1})
            else:
                node_dict.update({"category": 2})
            for key in input_dict.keys():
                if "相邻节点" in key:
                    other_nodes = input_dict[key]
        
        else:
            name = other_nodes[i-1].split('_')[0]
            num = other_nodes[i-1].split('_')[1]
            node_dict.update({"id": i, "name": name})
            
            if num[0] == 'a':
                node_dict.update({"category": 0})
                node_dict.update({"symbolSize": 38})
            elif num[0] == 'b':
                node_dict.update({"category": 1})
                node_dict.update({"symbolSize": 28})
            else:
                node_dict.update({"category": 2})
                node_dict.update({"symbolSize": 18})
            output_dict["links"].append({"source": "0", "target": str(i)})

        output_dict["nodes"].append(node_dict)
    # 将节点信息传入dict
    output_dict["node_info"] = node_info
    with open('test.json', 'w', encoding='utf-8') as fw:
        json.dump(output_dict,  fw, indent=4, ensure_ascii=False)
    return output_dict

def output2json(nodes):
    output_dict = {
                    "categories": [
                    {
                        "name": "核心节点"
                    },
                    {
                        "name": "次核心节点"
                    },
                    {
                        "name": "外围节点"
                    }
                    ],
                    "nodes": [], "links": []}
    nodes_no_nei = [{k: v for k, v in d.items() if k != 'neighbors'} for d in nodes]
    name_list = [node_['name'] for node_ in nodes]
    output_dict["nodes"] = nodes_no_nei

    for node in nodes:
        index = node['id']
        for neighbor in node['neighbors']:
            nei_name = neighbor.split('_')[0]
            if nei_name in name_list:
                nei_id = name_list.index(nei_name)
                output_dict["links"].append({"source": str(index), "target": str(nei_id)})

    with open('nodes_test.json', 'w', encoding='utf-8') as fw:
        json.dump(output_dict,  fw, indent=4, ensure_ascii=False)

    return len(output_dict['links'])

if __name__ == '__main__':

    index_path = '/root/node_index_dir'
    lucene.initVM()
    directory = store.FSDirectory.open(File(index_path).toPath()) 
    reader = DirectoryReader.open(directory) 
    searcher = IndexSearcher(reader) 

    print('-'*40)
    node_a =  ['人类命运共同体', '全球治理', '国际合作', '和平发展', '美美与共', '全球经济增长', '一带一路', '金砖合作', 
                '外交关系', '高峰论坛', '新兴经济体', '贸易保护主义', '数字丝路', '上合组织', '中美关系', '领袖峰会', '国家大数据', '物联网产业', '共同繁荣', '清洁美丽' ]
    node_b = ['全球经济治理', '中国制度', '美国单边主义', '逆全球化', '经济全球化', '经济协调发展', '全球贸易体系', '金融治理',
                '数字经济创新', '国际化合作', '网上丝路', '区域合作机制', '科技交流', '高端人才培养', '经济互惠', '经济全球化', '国际经济秩序', 
                '开展国际交流', '大数据创新', '共享文明', '技术应用创新', '产学研用合作', '投资与贸易', '区域合作发展', '现有国际秩序', '世界局势',
                '国际产业合作', '产业交流合作', '双向投资合作', '共建文明城市', '两大经济体', '协作创新', '美国的外交政策', '中国经济增长', '新兴市场国家', 
                '全球经济表现', '机器人产业', '区域经济一体化', '深化对外开放', '新一代信息产业', '全方位对外开放', '全面开放新格局', '发展论坛', '开放发展',
                '国家之间的关系', '贸易来往', '全球峰会', '生态大会', '新兴市场货币', '发展中经济体']
    node_c = ['全球价值观', '文化互鉴', '全球化时代', '维护国际秩序', '构建新型国际关系', '全球治理改革', '参与全球经济治理', '中国发展模式', '地区经济一体化', '维护世界和平与安全',
                '开展国际合作', '国际人文交流', '人文交流', '共建开放型世界经济', '加强互联互通', '多边主义和自由贸易', '中国的和平发展', '维护地区和平稳定', '做世界和平的建设者', '世界的发展',
                '双边交流', '促进国际交流', '应对全球挑战', '政策对话', '提升国际影响力', '美国经济展望', '环球经济', '增速预期', '贸易政策的不确定性', '石油消费',
                '一带一路经济', '深化经贸合作', '经贸合作平台', '基础设施合作', '国际国内双循环', '文明交流互鉴', '新型国际关系', '维护自由贸易', '深化各领域合作', '各领域互利合作',
                '经济和贸易', '主权和独立', '外交方针', '外交策略', '参与国际事务', '高峰论坛暨', '系列研讨会', '创新成果发布', '成果发布会', '创新高峰',
                '成熟经济体', '新兴工业国家', '中等收入国家', '国际经济组织', '发展中地区', '其他国家的贸易', '全球化浪潮', '贸易救济措施', '多边主义和自由贸易', '产业回流',
                '全球互联互通', '构建网络空间命运共同体', '物联网与智慧城市', '文化产业合作', '开放合作平台', '国际减贫合作', '中国与联合国', '深化经贸合作', '两国经贸合作', '发展战略对接',
                '中美关系的未来', '构建新型大国关系', '国际政治经济', '两岸关系和平发展', '中美经贸谈判', '全球智能化商业', '营销峰会', '电商大会', '创新与投资', '颁奖盛典',
                '国家数据中心', '大数据公共服务平台', '新型互联网交换中心', '国家超算', '国家数据中心', '超高清产业', '数字能源', '中国区块链产业', '数据中心行业', '鲲鹏产业生态',
                '推动两岸关系和平发展', '文明多样性', '深化交流合作', '促进繁荣发展', '共享发展成果', '碧水蓝天净土', '践行绿色发展', '人与自然和谐共生', '舒适的宜居环境', '生态美好'
                ]
    nodes = []
    index = 0
    for entity in node_a:
        node = {}
        result = search_node(entity, searcher)
        node['id'] = index
        node['name'] = result['关键词']
        node['category'] = 0
        node['symbolSize'] = 38
        node['neighbors'] = result['相邻节点']
        nodes.append(node)
        index += 1

    for entity in node_b:
        node = {}
        result = search_node(entity, searcher)
        node['id'] = index
        node['name'] = result['关键词']
        node['category'] = 1
        node['symbolSize'] = 28
        node['neighbors'] = result['相邻节点']
        nodes.append(node)
        index += 1

    for entity in node_c:
        node = {}
        result = search_node(entity, searcher)
        node['id'] = index
        node['name'] = result['关键词']
        node['category'] = 2
        node['symbolSize'] = 18
        node['neighbors'] = result['相邻节点']
        nodes.append(node)
        index += 1

    # with open('nodes_test.json', 'w', encoding='utf-8') as fw:
    #     json.dump(nodes,  fw, indent=4, ensure_ascii=False)
    link_num = output2json(nodes)
    print(link_num)

    reader.close()