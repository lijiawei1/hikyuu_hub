from workflow.processor import BaseProcessor


class DataValidator(BaseProcessor):
    """数据验证处理器，如果验证失败则中断工作流"""

    def process(self, data):
        self.logger.info("验证数据...")

        # 示例验证逻辑
        if data is None:
            self.logger.error("数据为空，验证失败")
            return data, False  # 中断工作流

        if 'required_field' not in data:
            self.logger.error("缺少必需字段，验证失败")
            return data, False  # 中断工作流

        self.logger.info("数据验证通过")
        return data, True  # 继续执行


class DataProcessor(BaseProcessor):
    """数据处理处理器，总是继续执行"""

    def process(self, data):
        self.logger.info("处理数据...")
        # 处理逻辑...
        processed_data = {"processed": True, **data}
        return processed_data, True  # 继续执行


class ConditionalProcessor(BaseProcessor):
    """条件处理器，根据条件决定是否继续"""

    def process(self, data):
        self.logger.info("执行条件处理...")

        # 根据某些条件决定是否继续
        if data.get('value', 0) > 100:
            self.logger.info("值大于100，继续执行")
            data['decision'] = 'continue'
            return data, True
        else:
            self.logger.info("值小于等于100，中断工作流")
            data['decision'] = 'stop'
            return data, False


class FinalProcessor(BaseProcessor):
    """最终处理器，不需要返回继续标志"""

    def process(self, data):
        self.logger.info("执行最终处理...")
        # 处理逻辑...
        data['finalized'] = True
        return data  # 不返回继续标志，默认继续


