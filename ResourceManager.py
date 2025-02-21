import asyncio
from collections import defaultdict
import logging

class ResourceManager:
    """
    资源管理器：使用队列和锁来管理对不同服务的有序和互斥访问
    """
    def __init__(self):
        self.resource_locks = defaultdict(asyncio.Lock)  # 资源锁，确保 GPU/ArcGIS 资源的独占访问

        self.service_onusage_dag_records = {}  # 记录当前持有服务锁的 DAG

        self.service_locks = defaultdict(asyncio.Lock)  # 服务锁，确保每个服务只能由一个 DAG 访问
        self.service_queues = defaultdict(asyncio.Queue)  # 队列，存放服务请求
        # self.service_usage_counts = defaultdict(int)  # 服务的使用次数，用于判断锁的释放时机
        self.resource_usage_counts = defaultdict(int)  # 资源的使用计数，用于判断资源锁的注销时机

    async def add_request_to_queue(self, service_name, dag_id, request, service_usage_count, forwarder, task):
        """
        将请求添加到特定服务的队列中，并注册使用次数
        """
        await self.service_queues[service_name].put((dag_id, request, service_usage_count, forwarder, task))

    async def acquire_resource_lock(self, service_name, dag_id, resource_type):
        """
        尝试获取资源锁，如果已被其他任务占用则等待
        """
        # 如果该 DAG 已经持有服务锁，无需等待
        if self.service_onusage_dag_records.get(service_name) == dag_id:                            
            return
                
        # 获取资源锁（如 GPU 或 ArcGIS）
        resource_lock = self.resource_locks[resource_type]
        await resource_lock.acquire()

        # 获取服务锁，确保该 DAG 独占访问该服务
        service_lock = self.service_locks[service_name]
        await service_lock.acquire()

        # 记录当前服务由哪个 DAG 持有
        self.service_onusage_dag_records[service_name] = dag_id
        

    async def process_requests(self, service_name, dag_id ,resource_type):
        """
        处理请求队列中的请求，确保按顺序处理，并使用锁进行互斥控制
        """
        queue = self.service_queues[service_name]
        found_task_for_dag = False

        # 遍历队列，寻找当前 DAG 的任务
        for _ in range(queue.qsize()):
            current_dag_id, request_body, service_usage_count, forwarder, task = await queue.get()

            # 检查当前任务是否属于当前 DAG
            if current_dag_id == dag_id:
                found_task_for_dag = True      
                try:                    
                    # 使用转发器将请求转发到相应的服务
                    response = forwarder.forward(task, request_body)
                    logging.info(f"Processed request for {service_name} with task {task}. Remaining usage count: {service_usage_count - 1}")
                    
                    # 如果檢查發現使用次数用完了，释放资源锁和服务锁
                    service_usage_count -= 1 
                    
                    if service_usage_count == 0:                      
                        # 释放服务锁和资源锁
                        await self.release_service_lock(service_name)
                        await self.release_resource_lock(resource_type)                         
                    
                    return response
                    
                except Exception as e:
                    logging.error(f"Error processing request for {service_name}: {e}")
                    continue                               
            else:
                # 如果不是当前 DAG 的任务，重新放回队列
                await queue.put((current_dag_id, request_body, service_usage_count, forwarder, task))

        if not found_task_for_dag:
            logging.info(f"No tasks for DAG {dag_id} found in {service_name}'s queue.")


    async def register_locks(self, service_name, resource_name):
        """
        注册服务锁和资源锁
        """
        # 確認服務鎖是否存在， 如果沒有則註冊
        if service_name not in self.service_locks:
            self.service_locks[service_name] = asyncio.Lock()
            logging.info(f"Service lock registered for {service_name}")

        # 確認資源鎖是否存在， 如果沒有則註冊
        if resource_name not in self.resource_locks:
            self.resource_locks[resource_name] = asyncio.Lock()
            logging.info(f"Resource lock registered for {resource_name}")

        # 增加资源的使用计数 (== 使用該resource之服務數量統計)
        self.resource_usage_counts[resource_name] += 1

    async def unregister_locks(self, service_name, resource_name):
        """
        注销服务锁和资源锁
        """
        # 减少资源的使用计数
        self.resource_usage_counts[resource_name] -= 1

        # 如果没有服务再使用该资源，释放资源锁
        if self.resource_usage_counts[resource_name] == 0:
            await self.release_resource_lock(resource_name)
            logging.info(f"Resource lock for {resource_name} has been released since no services are using it.")

        # 清除服务锁
        if service_name in self.service_locks:
            del self.service_locks[service_name]
        logging.info(f"Service lock for {service_name} has been unregistered.")

    async def release_resource_lock(self, resource_name):
        """
        释放资源锁（如 GPU 或 ArcGIS）
        """
        resource_lock = self.resource_locks[resource_name]
        if resource_lock.locked():
            resource_lock.release()

    async def release_service_lock(self, service_name):
        """
        释放服务锁并清空持有者信息
        """
        service_lock = self.service_locks[service_name]
        if service_lock.locked():
            service_lock.release()
        if service_name in self.service_onusage_dag_records:
            del self.service_onusage_dag_records[service_name]