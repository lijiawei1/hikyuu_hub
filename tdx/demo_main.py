from tdx.demo_workflow import DataValidator, ConditionalProcessor
from workflow import Config, WorkflowLogger, Workflow,BaseProcessor


# 自定义处理器
class DataLoader(BaseProcessor):
    def process(self, data):
        self.logger.info("加载数据...")
        # 模拟加载数据
        return {"sample": "data", "value": 150}, True


class DataTransformer(BaseProcessor):
    def process(self, data):
        self.logger.info("转换数据...")
        data["transformed"] = True
        return data, True


class DataSaver(BaseProcessor):
    def process(self, data):
        self.logger.info("保存数据...")
        self.logger.info(f"数据已保存: {data}")
        return data  # 不返回继续标志，默认继续


def main():
    # 加载配置
    config = Config("workflow/example_config.yaml")

    # 初始化日志
    logger = WorkflowLogger(
        name="conditional_workflow",
        log_level=config.get("logging", {}).get("level", "INFO"),
        log_file=config.get("logging", {}).get("file")
    )

    # 创建工作流
    workflow = Workflow(config, logger)

    # 添加处理器
    workflow.add_processor(DataLoader(config, logger))
    workflow.add_processor(DataValidator(config, logger))  # 验证处理器
    workflow.add_processor(ConditionalProcessor(config, logger))  # 条件处理器
    workflow.add_processor(DataTransformer(config, logger))
    workflow.add_processor(DataSaver(config, logger))

    try:
        # 初始化工作流
        workflow.setup()

        # 执行工作流
        result = workflow.execute()

        if workflow.should_continue:
            logger.info(f"工作流完整执行，结果: {result}")
        else:
            logger.info(f"工作流被中断，当前结果: {result}")

    except Exception as e:
        logger.error(f"工作流执行失败: {e}")
    finally:
        # 清理工作流
        workflow.teardown()


if __name__ == "__main__":
    main()