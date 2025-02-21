from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import requests
import uvicorn
import logging
from ServiceForwarderFactory import  ServiceForwarderFactory
from ResourceManager import ResourceManager
import asyncio
from collections import defaultdict
from Forwarders import Forwarder

# 配置日志
logging.basicConfig(level=logging.DEBUG)

# 配置 Node Manager 的 URL 和节点信息
NODE_MANAGER_URL = "http://192.168.158.43:8000"  # 替换为Node Manager的实际地址和端口

# 節點資訊
NODE_ID = "machineA"
NODE_IP = "http://10.52.52.137"
NODE_PORT = 8080
NODE_CAPACITY= 3

# 本地服务注册表，用于管理节点内服务的注册信息
services_registry = {}

# 初始化 FastAPI 应用
app = FastAPI()

# 初始化服务转发工厂
forwarder_factory = ServiceForwarderFactory()

resource_manager = ResourceManager()

# 数据模型
class RegisterServiceRequest(BaseModel):
    service_name: str
    service_address: str
    service_port: int
    service_resource_type: str  # 例如: "CPU" 或 "GPU"

# 数据模型
class ExecuteNodeServiceRequest(BaseModel):
    dag_id: str
    execution_id: str
    dag_folder: str
    SHP_Path: str
    TIF_Path: str
    model: str
    model_download_url: str

def register_node_to_manager(node_ip):
    """
    向 Node Manager 注册节点信息
    """
    url = f"{NODE_MANAGER_URL}/node/register"
    node_data ={
        "node_id": NODE_ID,
        "node_ip": NODE_IP,
        "node_port": NODE_PORT,
        "node_capacity": NODE_CAPACITY
        }

    response = requests.post(url, json=node_data)
    if response.status_code == 200:
        logging.info("Node registered successfully.")
    else:
        logging.error(f"Failed to register node: {response.text}")

def clean_node_info_on_manager(node_ip):
    """
    向 Node Manager 注銷节点信息
    """
    url = f"{NODE_MANAGER_URL}/node/cleanup"
    node_data ={
        "node_id": NODE_ID,
        "node_ip": NODE_IP,
        "node_port": NODE_PORT,
        "node_capacity": NODE_CAPACITY
        }

    response = requests.post(url, json=node_data)
    if response.status_code == 200:
        logging.info("Node unregistered successfully.")
    else:
        logging.error(f"Failed to unregister node: {response.text}")
    
    return response

def register_new_service_to_node_manager(service_name, service_address ,service_port, service_resource_type):
    """
    通知 Node Manager 节点上的新服务
    """
    url = f"{NODE_MANAGER_URL}/service/register"
    service_info = {
        "service_node_id": NODE_ID,
        "service_name": service_name,
        "service_address": NODE_IP,
        "service_port": service_port,
        "service_resource_type": service_resource_type
        }
    response = requests.post(url, json=service_info)
    if response.status_code == 200:
        logging.info(f"Node Manager notified of new service {service_name} on port {service_port}.")
    else:
        logging.error(f"Failed to notify Node Manager: {response.text}")

    # 返回 response 对象
    return response

def unregister_service_to_node_manager(service_name, service_address,service_port, service_resource_type):
    """
    通知 Node Manager 註銷節點服务
    """
    url = f"{NODE_MANAGER_URL}/service/cleanup"
    service_info = {
        "service_node_id": NODE_ID,
        "service_name": service_name,
        "service_address": service_address,
        "service_port": service_port,
        "service_resource_type": service_resource_type
        }
    response = requests.post(url, json=service_info)
    if response.status_code == 200:
        logging.info(f"Node Manager notified to cleanup service {service_name} on port {service_port}.")
    else:
        logging.error(f"Failed to notify Node Manager to cleanup service: {response.text}")

    # 返回 response 对象
    return response


async def process_service_requests_in_order(service_name, lock, queue):
    """
    依次处理特定服务的请求队列，并确保互斥访问
    """
    async with lock:
        while not queue.empty():
            request_body = await queue.get()

            try:
                # 获取服务的转发器
                forwarder = forwarder_factory.get_forwarder(service_name)

                # 使用转发器将请求转发到相应的服务
                response = forwarder(request_body)

                logging.info(f"Processed request for {service_name}")
                # 返回响应给客户端
                return response

            except Exception as e:
                logging.error(f"Error processing request for {service_name}: {e}")
                continue

@app.on_event("startup")
async def startup_event():
    """
    启动时注册节点信息到 Node Manager
    """
    try:
        node_ip = NODE_IP  # 替换为实际的节点IP
        register_node_to_manager(node_ip)
    except Exception as e:
        logging.error(f"Error during node registration: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """
    当Node Agent关闭时，注销所有注册的服务，并注销节点
    """
    try:
        # 注销所有服务
        for service_name, service_info in list(services_registry.items()):
            response = unregister_service_to_node_manager(
                service_name,
                service_info['service_address'],
                service_info['service_port'],
                service_info['service_resource_type']
            )
            if response.status_code == 200:
                logging.info(f"Service {service_name} successfully unregistered from Node Manager.")
            else:
                logging.error(f"Failed to unregister service {service_name}: {response.content}")

            # 註銷转发代理
            forwarder_factory.remove_forwarder(service_name)
            logging.info(f"Forwarder of Service {service_name} has been removed")

            # 注销服务锁和资源锁
            await resource_manager.unregister_locks(service_name, service_info['service_resource_type'])
            logging.info(f"Locks for service {service_name} and resource {service_info['service_resource_type']} have been unregistered")

        node_id = NODE_ID
        response = clean_node_info_on_manager(node_id)

        if response.status_code == 200:
            logging.info("Node successfully unregistered from Node Manager.")
        else:
            logging.error(f"Failed to unregister node from Node Manager: {response.content}")

    except Exception as e:
        logging.error(f"Error during node unregistration: {e}")

#［向節點agent註冊服務，並由節點代為轉註冊於node manager］
@app.post("/service/register")
async def register_service(request: RegisterServiceRequest):
    """
    接收服务注册请求
    """
    try:
        # 提取请求中的服务参数
        service_node_id = NODE_ID  # 节点 ID
        service_name = request.service_name
        service_address = request.service_address
        service_port = request.service_port
        service_resource_type = request.service_resource_type
        
        # 在本地注册表中注册服务
        services_registry[service_name] = {
            "service_node_id": service_node_id,
            "service_name": service_name,
            "service_address": service_address,
            "service_port": service_port,
            "service_resource_type": service_resource_type
        }
        logging.info(f"Service {service_name} registered locally on port {service_port} with resource {service_resource_type}.")

        # 通知 Node Manager 新服务的注册
        response = register_new_service_to_node_manager(service_name, service_address, service_port, service_resource_type)
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to notify Node Manager of new service")

        # 使用工厂方法创建 Forwarder 对象并注册
        forwarder = forwarder_factory.create_forwarder(service_name, service_address, service_port)
        forwarder_factory.register_forwarder(service_name, forwarder)
        logging.info(f"Forwarder of Service {service_name} has been created")

        # 动态注册资源锁和服务锁
        await resource_manager.register_locks(service_name, service_resource_type)
        logging.info(f"Resource lock for {service_name} and resource type {service_resource_type} has been registered")

        return {"message": f"Service {service_name} registered successfully"}

    except Exception as e:
        logging.error(f"Error registering service: {e}")
        raise HTTPException(status_code=500, detail=str(e))

#［向節點agent註銷服務，並由節點代為轉註銷於node manager］
@app.post("/service/unregister")
async def unregister_service(request: RegisterServiceRequest):
    """
    接收服务注销请求
    """
    try:
        service_name = request.service_name
        service_port = request.service_port
        service_resource_type = request.service_resource_type
        service_address = request.service_address

        # 获取注册服务信息
        service_registry_on_agent_info = services_registry.get(service_name)
        if not service_registry_on_agent_info:
            raise HTTPException(status_code=404, detail="Service not found")

        # 向 Node Manager 发送服务注销请求
        response = unregister_service_to_node_manager(service_name, service_address, service_port, service_resource_type)
        if response.status_code != 200:
            logging.error(f"Failed to unregister service from Node Manager: {response.content}")
            raise HTTPException(status_code=500, detail=f"Failed to unregister service from Node Manager: {response.content}")

        logging.info(f"Service {service_name} successfully unregistered from Node Manager.")

        # 停止转发代理
        forwarder_factory.remove_forwarder(service_name)
        logging.info(f"Forwarder of Service {request.service_name} has been removed")

        # 清除本地注册的服务信息
        del services_registry[service_name]
        logging.info(f"Information of Service {request.service_name} has been deleted")

        # 注销服务锁和资源锁
        await resource_manager.unregister_locks(service_name, service_resource_type)
        logging.info(f"Locks for service {service_name} and resource {service_resource_type} have been unregistered")

        return {"message": f"Service {service_name} unregistered successfully"}

    except Exception as e:
        logging.error(f"Error unregistering service: {e}")
        raise HTTPException(status_code=500, detail=str(e))

#［處理來自airflow dag 的請求 並轉發於註冊其上之服務，同時做資源管理］
@app.post("/api/{service_type}/{task}")
async def handle_request(service_type: str, task: str, request: Request):
    """
    处理请求并按顺序转发到相应的服务，同时确保互斥访问
    """
    try:
        # 從請求中獲取相關數據
        request_data = await request.json()
        dag_id = request_data.get('DAG_ID')  # 獲取 DAG ID
        service_usage_count = request_data.get('SERVICE_USAGE_COUNT', 1)  # 使用計數，默認為1
        service_resource_type = request_data.get('SERVICE_USAGE_RESOURCE_TYPE')
        logging.info(f"Received request for service: {service_type} with task: {task}, dag_id: {dag_id}, usage_count: {service_usage_count}")
        logging.info(f"Request body: {request_data}")

        # 獲取對應的forwarder
        forwarder = forwarder_factory.get_forwarder(service_type)

        # 將請求加入該服務之queue
        await resource_manager.add_request_to_queue(service_type, dag_id, request_data, service_usage_count, forwarder, task)
        
        # 嘗試獲取資源鎖與服務鎖
        await resource_manager.acquire_resource_lock(service_type, dag_id, service_resource_type)

        # 嘗試執行請求轉發與回傳服務響應
        response = await resource_manager.process_requests(service_type, dag_id, service_resource_type)

        return response

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logging.error(f"Error forwarding request for {service_type}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error forwarding request: {str(e)}")

if __name__ == "__main__":
    # 启动 FastAPI 应用
    uvicorn.run(app, host="0.0.0.0", port=8080, reload=True)
