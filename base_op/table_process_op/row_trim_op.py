from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class RowTrimOp(Op):
    """
    【local】行截断算子，支持选择保留头部或尾部的行，仅支持创建新表，不支持原地操作

    Attributes:
        reserve_num (int): 保留行数
        trim_type (str): 截断类型，可选值为 "head" 或 "tail"
        table_name (str, optional): 新表的名称，默认为 "trimmed_{in_table.name}"

    InputTables:
        in_table: 输入表格

    OutputTables:
        new_table: 截断后的新表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/row_trim_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.row_trim_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                'id':[1,2,3],
                'nums':[12,23,35]
            }
        )
        display(input_table)

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子,
        #reserve_num表示保留的行数
        #trim_type有两种方式，保留head和保留tail
        row_trim_op = RowTrimOp(
            name="RowTrimOp",
            attrs={
                "table_name": "new_table",
                "reserve_num": 2,
                "trim_type":"tail"
            }
        )

        # 执行算子
        success = row_trim_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        reserve_num = self.attrs.get("reserve_num")
        trim_type = self.attrs.get("trim_type", "head")
        table_name = self.attrs.get("table_name", f"trimmed_{in_table.name}")

        if trim_type == "head":
            new_df = in_table.head(reserve_num)
            new_table = DataTable(name=table_name, data=new_df)
        else:  # tail
            new_df = in_table.tail(reserve_num)
            new_table = DataTable(name=table_name, data=new_df)

        op_context.output_tables.append(new_table)
        return True


op_register.register_op(RowTrimOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="reserve_num", type="int", desc="保留行数") \
    .add_attr(name="trim_type", type="str", desc="截断类型") \
    .add_attr(name="table_name", type="str", desc="新表名")
