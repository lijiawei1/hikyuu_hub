import yaml
import os
from typing import Dict, Any


class Config:
    def __init__(self, config_path: str = None):
        self.config = {}
        if config_path and os.path.exists(config_path):
            self.load_config(config_path)

    def load_config(self, config_path: str):
        """加载YAML配置文件"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            raise Exception(f"加载配置文件失败: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        return self.config.get(key, default)

    def set(self, key: str, value: Any):
        """设置配置值"""
        self.config[key] = value

    def __getitem__(self, key: str) -> Any:
        return self.config[key]

    def __contains__(self, key: str) -> bool:
        return key in self.config