import requests
import logging

class Forwarder:
    def __init__(self, address, port):
        # 直接构建不带多余 'http://' 的 URL
        if not address.startswith("http"):
            address = f"http://{address}"
        self.service_url = f"{address}:{port}"
        
    def forward(self, task, request_body):
        raise NotImplementedError("Subclasses should implement this method.")

    def __call__(self, task, request_body):
        """
        使得 Forwarder 实例可调用，调用 forward 方法。
        """
        return self.forward(task, request_body)
    
    # 公用的响应处理方法
    def _handle_response(self, response):
        """
        处理响应并检查错误
        """
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Error response from service: {response.status_code} - {response.text}")
            response.raise_for_status()

class InferenceForwarder(Forwarder):
    """
    Inference 服务转发器
    """
    def forward(self, task, request_body):
        try:
            full_url = f"{self.service_url}/Inference/{task}"
            logging.info(f"Forwarding request to {full_url} with data: {request_body}")
            response = requests.post(full_url, json=request_body)
            return self._handle_response(response)
        except Exception as e:
            logging.error(f"Error forwarding request to Inference service: {e}")
            raise e

class TrainingForwarder(Forwarder):
    """
    Training 服务转发器
    """
    def forward(self, task, request_body):
        try:
            full_url = f"{self.service_url}/Training/{task}"
            logging.info(f"Forwarding request to {full_url} with data: {request_body}")
            response = requests.post(full_url, json=request_body)
            return self._handle_response(response)
        except Exception as e:
            logging.error(f"Error forwarding request to Training service: {e}")
            raise e

class PreprocessingForwarder(Forwarder):
    """
    Preprocessing 服务转发器
    """
    def forward(self, task, request_body):
        try:
            # 构建完整的请求 URL
            full_url = f"{self.service_url}/Preprocessing/{task}"
            logging.info(f"Forwarding request to {full_url} with data: {request_body}")
            response = requests.post(full_url, json=request_body)
            return self._handle_response(response)
        except Exception as e:
            logging.error(f"Error forwarding request to Preprocessing service: {e}")
            raise e

class PostprocessingForwarder(Forwarder):
    """
    Postprocessing 服务转发器
    """
    def forward(self, task, request_body):
        try:
            full_url = f"{self.service_url}/Postprocessing/{task}"
            logging.info(f"Forwarding request to {full_url} with data: {request_body}")
            response = requests.post(full_url, json=request_body)
            return self._handle_response(response)
        except Exception as e:
            logging.error(f"Error forwarding request to Postprocessing service: {e}")
            raise e

class ClusteringForwarder(Forwarder):
    """
    Clustering 服务转发器
    """
    def forward(self, task, request_body):
        try:
            full_url = f"{self.service_url}/Clustering/{task}"
            logging.info(f"Forwarding request to {full_url} with data: {request_body}")
            response = requests.post(full_url, json=request_body)
            return self._handle_response(response)
        except Exception as e:
            logging.error(f"Error forwarding request to Clustering service: {e}")
            raise e

