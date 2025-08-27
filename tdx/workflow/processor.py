import os
from abc import ABC, abstractmethod
from .logger import WorkflowLogger
from .config import Config


class BaseProcessor(ABC):
    def __init__(self, config: Config, logger: WorkflowLogger):
        self.config = config
        self.logger = logger

    @abstractmethod
    def process(self, data: any) -> (any, bool):
        """
        处理数据的方法，需要子类实现

        Args:
            data: 输入数据

        Returns:
            tuple: (处理后的数据, 是否继续执行下一个处理器)
                   - 如果返回True，继续执行下一个处理器
                   - 如果返回False，终止工作流执行
        """
        pass

    def setup(self):
        """初始化方法，可选实现"""
        pass

    def teardown(self):
        """清理方法，可选实现"""
        pass

    def get_tdx_base_path(self):
        return self.config.get("path", {}).get("tdx_base_path")

    def get_work_tdx_path(self):
        return self.config.get("path", {}).get("work_tdx_path")

    def get_info_file_path(self):
        tdx_base_path = self.get_tdx_base_path()
        info_file = self.config.get("path", {}).get("info_file")
        info_file_path = os.path.join(tdx_base_path, info_file)
        return info_file_path

    def get_idx_file_path(self, name:str, work:bool=False):
        tdx_base_path = self.get_work_tdx_path() if work else self.get_tdx_base_path()
        idx_file = self.config.get("path", {}).get(name)
        idx_file_path = os.path.join(tdx_base_path, idx_file)
        return idx_file_path

    def get_dat_file_path(self, name:str, work:bool=False):
        tdx_base_path =  self.get_work_tdx_path() if work else self.get_tdx_base_path()
        dat_file = self.config.get("path", {}).get(name)
        dat_file_path = os.path.join(tdx_base_path, dat_file)
        return dat_file_path

    def get_temp_extdata_path(self):
        return self.config.get("path", {}).get("temp_extdata_path")

    def get_flag_sum_mapping(self):
        return self.config.get("path", {}).get("flag_sum_mapping")

    def get_copy_ranges(self):
        return self.config.get("path", {}).get("copy_ranges")
    # def get_idx_file_path(self):
    #     tdx_base_path = self.get_tdx_base_path()
    #     info_file = self.config.get("path", {}).get("info_file")
    #     info_file_path = os.path.join(tdx_base_path, info_file)
    #     return info_file_path

    def get_append_list(self):
        return self.config.get("path", {}).get("append_list")