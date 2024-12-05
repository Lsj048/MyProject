from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class PerfOp(Op):
    """
    【local】Perf打点算子，用于监控性能指标和业务指标

    Attributes:
        subtag (str): 打点的子标签，默认为空字符串。
        extra1 (str): 额外参数1，默认为空字符串。
        extra2 (str): 额外参数2，默认为空字符串。
        extra3 (str): 额外参数3，默认为空字符串。
        extra4 (str): 额外参数4，默认为空字符串。
        value_column (str): 值所在的列名，默认为None。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        in_table: 输出表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/monitor_op/perf_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.monitor_op.perf_op import *

        # 创建一个包含性能指标的模拟数据表
        input_table = DataTable(
            name = "TestTable",
            data = {
                '响应时间':[1.2,2.0,3.5,4.2],
                '处理时间':[1.6,2.3,12.0,43]
            }
        )

        # 设置 OpContext
        op_context = OpContext("graph_name", "request_tag", "request_id")
        op_context.input_tables.append(input_table)

        # 实例化 PerfOp 并运行，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
        perf_op = PerfOp(
            name='PerfOp',
            attrs={
            "value_column": ["响应时间","处理时间"],
            }
        )
        success = perf_op.process(op_context)

        # 检查结果
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子计算失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        subtag = self.attrs.get("subtag", "")
        extra1 = self.attrs.get("extra1", "")
        extra2 = self.attrs.get("extra2", "")
        extra3 = self.attrs.get("extra3", "")
        extra4 = self.attrs.get("extra4", "")
        value_column = self.attrs.get("value_column", None)

        if value_column:
            for index, row in in_table.iterrows():
                value = row.get(value_column)
                if isinstance(value, (int, float)):
                    op_context.perf_ctx(subtag=subtag, micros=value, extra1=extra1, extra2=extra2, extra3=extra3,
                                        extra4=extra4)
        else:
            op_context.perf_ctx(subtag=subtag, extra1=extra1, extra2=extra2, extra3=extra3, extra4=extra4)

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(PerfOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="subtag", type="str", desc="subtag") \
    .add_attr(name="extra1", type="str", desc="extra1") \
    .add_attr(name="extra2", type="str", desc="extra2") \
    .add_attr(name="extra3", type="str", desc="extra3") \
    .add_attr(name="extra4", type="str", desc="extra4") \
    .add_attr(name="value_column", type="str", desc="统计值的列")
