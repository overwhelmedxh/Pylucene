#-*-coding:utf-8-*- 
from fastapi import FastAPI,status
import uvicorn
import json
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import node_search_n
import para_search_n
import text_search_n
import lucene
from java.io import File
from org.apache.lucene.search import IndexSearcher,TermQuery
from org.apache.lucene import store, queryparser
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.analysis.standard import StandardAnalyzer



app = FastAPI(
    title="Single Page API",
    description="Temp use",
    version="0.1",
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"Hello": "World"}
   
# lucene.initVM()

@app.get('/api/node_search')
def node_search(keyword:str,num:int):
    print("keyword:" + keyword)
    if "all" == keyword:
        with open("nodes_test.json", 'r', encoding='utf-8') as load_file:
            load_info = json.load(load_file)
            load_info["node_info"] = {}
        return JSONResponse(status_code=status.HTTP_200_OK, content=load_info)
    try:
        index_path = '/root/node_index_dir'
        if lucene.getVMEnv()==None:
            lucene.initVM()
        directory = store.FSDirectory.open(File(index_path).toPath()) 
        reader = DirectoryReader.open(directory) 
        searcher = IndexSearcher(reader) 

        print('-'*40)
        # entity = input('请输入关键词（如人类命运共同体）：\n')
        entity = keyword

        result = node_search_n.search_node(entity, searcher, num) # 20为需要返回的邻节点个数，可调节
        if len(result)==0:
            return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={})
        result = node_search_n.transform_node(result, num)

        reader.close()

        # 这里写入逻辑
        # data = {"keyword":keyword,"name":"123"}
        print(result)
        return JSONResponse(status_code=status.HTTP_200_OK, content=result)
    except Exception as e:
        print("error:",e)
        # reader.close()
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={})
    
@app.get('/api/para_search')
def para_search(para_id:str):
    print("para_id:" + para_id)
    try:
        # index_path = '/root/data/index_dir/content_index'
        # lucene.initVM()
        # directory = store.FSDirectory.open(File(index_path).toPath()) 
        # reader = DirectoryReader.open(directory) 
        # searcher = IndexSearcher(reader) 
        # analyzer = StandardAnalyzer() 
        # parser = queryparser.classic.QueryParser('para_id', analyzer)

        # # para_id = input('输入段落编号（形如d20211110041p008）：\n')
        # result_data = para_search_n.search_para(para_id, parser, searcher)
        # result_data = para_search_n.para_search(para_id)
        # reader.close()
        # 这里写入逻辑
        # data = {"para_id":para_id,"name":"123"}
        result_data = para_search_n.para_search(para_id)
        # print(result_data)
        return JSONResponse(status_code=status.HTTP_200_OK, content=result_data)
    except Exception as e:
        print("error:",e)
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={})

class Item(BaseModel):
    text: str
    num: int

@app.post('/api/text_search')
def text_search(item:Item):
    print("text:" + item.text)
    try:
        result_data = text_search_n.text_search(item.text,item.num)
        return JSONResponse(status_code=status.HTTP_200_OK, content=result_data)
    except Exception as e:
        print("error:",e)
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={})   

if __name__ == "__main__":
    # node_search(keyword="人类命运共同体",num=20)
    # para_search("d20180126059p001")
    # lucene.initVM()
    uvicorn.run("service:app", host="0.0.0.0", port=9200, log_level="info",reload=True)

