from video_graph.common.utils.redis import RedisManager
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class ExtractColumnFromRedisOp(Op):
    """
    【local】从 Redis 中提取列算子，仅支持字符串类型的 Value

    Attributes:
        redis_cluster (str): Redis 集群名称。
        redis_key_column (str): Redis Key 列名。
        redis_value_column (str): Redis Value 列名。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        in_table: 加入新列后的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/extract_column_from_redis_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.extract_column_from_redis_op import *

        # 创建输入表格，其中redis_key必须是真实存在的key
        input_table = DataTable(
            name="TestTable",
            data = {
                "redis_key":["kw3_144473676322"]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子，其中redis_cluster必须是真实存在的集群
        extract_column_from_redis_op = ExtractColumnFromRedisOp(
            name="ExtractColumnFromRedisOp",
            attrs={
                "redis_cluster": "adAigcDataCache",
                "redis_key_column": "redis_key",
                "redis_value_column": "redis_value"
            }
        )

        # 执行算子
        success = extract_column_from_redis_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        redis_cluster = self.attrs.get("redis_cluster")
        redis_key_column = self.attrs.get("redis_key_column")
        redis_value_column = self.attrs.get("redis_value_column")

        redis_client = RedisManager().get_client(redis_cluster)
        in_table[redis_value_column] = None
        for index, row in in_table.iterrows():
            redis_key = row.get(redis_key_column)
            if not redis_key:
                continue

            redis_value = redis_client.get(redis_key)
            in_table.at[index, redis_value_column] = redis_value.decode('utf-8')

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(ExtractColumnFromRedisOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="redis_cluster", type="str", desc="redis集群地址") \
    .add_attr(name="redis_key_column", type="str", desc="redis key列名") \
    .add_attr(name="redis_value_column", type="str", desc="redis value 列名")

