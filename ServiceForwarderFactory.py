import logging
from Forwarders import Forwarder, InferenceForwarder, PreprocessingForwarder, TrainingForwarder, PostprocessingForwarder, ClusteringForwarder

# 服务转发器的工厂类
class ServiceForwarderFactory:
    def __init__(self):
        self._forwarders = {}

    def create_forwarder(self, service_name, service_address, port):
        """
        簡單工厂方法，根据服务名称创建相应的 Forwarder 对象。
        """
        if service_name == "Inference":
            return InferenceForwarder(service_address,port)
        
        elif service_name == "Training":
            return TrainingForwarder(service_address, port)
        
        elif service_name == "Preprocessing":
            return PreprocessingForwarder(service_address, port)
        
        elif service_name == "Postprocessing":
            return PostprocessingForwarder(service_address, port)
        
        elif service_name == "Clustering":
            return ClusteringForwarder(service_address, port)
        else:
            raise ValueError(f"Unknown service type: {service_name}")
        

    def register_forwarder(self, service_name, forwarder_obj):
        self._forwarders[service_name] = forwarder_obj

    def remove_forwarder(self, service_name):
        if service_name in self._forwarders:
            del self._forwarders[service_name]

    def get_forwarder(self, service_name):
        forwarder = self._forwarders.get(service_name)
        if not forwarder:
            raise ValueError(f"Service {service_name} is not registered")
        return forwarder

