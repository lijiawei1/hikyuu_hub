from typing import List, Dict, Any, Tuple
from .logger import WorkflowLogger
from .config import Config
from .processor import BaseProcessor


class Workflow:
    def __init__(self, config: Config, logger: WorkflowLogger):
        self.config = config
        self.logger = logger
        self.processors: List[BaseProcessor] = []
        self.context: Dict[str, Any] = {}
        self.should_continue = True  # 控制是否继续执行的标志

    def add_processor(self, processor: BaseProcessor):
        """添加处理器到工作流"""
        self.processors.append(processor)

    def setup(self):
        """初始化所有处理器"""
        self.logger.info("开始初始化工作流")
        for processor in self.processors:
            try:
                processor.setup()
                self.logger.info(f"处理器 {processor.__class__.__name__} 初始化成功")
            except Exception as e:
                self.logger.error(f"处理器 {processor.__class__.__name__} 初始化失败: {e}")
                raise

    def execute(self, initial_data: Any = None) -> Any:
        """执行工作流"""
        self.logger.info("开始执行工作流")

        # 初始化上下文
        data = initial_data
        self.context['start_time'] = self.get_current_time()
        self.should_continue = True  # 重置继续执行标志

        try:
            # 依次执行所有处理器
            for i, processor in enumerate(self.processors):
                if not self.should_continue:
                    self.logger.info(f"工作流被处理器 {i} 中断")
                    break

                self.logger.info(f"执行处理器 {i + 1}/{len(self.processors)}: {processor.__class__.__name__}")

                try:
                    # 执行处理器并获取返回值和继续标志
                    result = processor.process(data)

                    # 处理返回值
                    if isinstance(result, tuple) and len(result) == 2:
                        data, continue_flag = result
                        self.should_continue = continue_flag
                    else:
                        # 向后兼容：如果处理器没有返回元组，只返回数据
                        data = result
                        self.should_continue = True

                    if self.should_continue:
                        self.logger.info(f"处理器 {processor.__class__.__name__} 执行成功，继续下一个处理器")
                    else:
                        self.logger.info(f"处理器 {processor.__class__.__name__} 执行成功，但中断工作流")

                except Exception as e:
                    self.logger.error(f"处理器 {processor.__class__.__name__} 执行失败: {e}")
                    raise

            # 记录完成时间
            self.context['end_time'] = self.get_current_time()
            self.context['duration'] = self.context['end_time'] - self.context['start_time']

            if self.should_continue:
                self.logger.info(f"工作流执行完成，耗时: {self.context['duration']} 秒")
            else:
                self.logger.info(f"工作流被中断，耗时: {self.context['duration']} 秒")

            return data

        except Exception as e:
            self.logger.error(f"工作流执行失败: {e}")
            raise

    def teardown(self):
        """清理所有处理器"""
        self.logger.info("开始清理工作流")
        for processor in self.processors:
            try:
                processor.teardown()
                self.logger.info(f"处理器 {processor.__class__.__name__} 清理成功")
            except Exception as e:
                self.logger.warning(f"处理器 {processor.__class__.__name__} 清理失败: {e}")

    def get_current_time(self):
        """获取当前时间（用于计时）"""
        import time
        return time.time()

    def interrupt(self):
        """中断工作流执行"""
        self.should_continue = False
        self.logger.info("工作流被中断")