from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class TableQueryOp(Op):
    """
    【local】基于pandas的query接口实现的表数据查询过滤算子
    相比于TableRowExtractOp和TableRowFilterOp，支持更灵活的语义
    用法参考：https://note.nkmk.me/en/python-pandas-query/

    Attributes:
        expression (str): 查询表达式
        inplace (bool, optional): 是否在原表上进行操作，默认为 False
        table_name (str, optional): 新表的名称，仅在inplace为False时有效
        engine (str, optional): 查询引擎，默认为 python
        reset_index (bool, optional): 是否重置索引，默认为 True

    InputTables:
        in_table: 输入表格

    OutputTables:
        in_table/new_table: 查询后的表格，如果 inplace 为 True，则输出表为原表，否则为新表

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/table_query_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.table_query_op import *

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

        # 配置并实例化算子,如果inplace为True则无需设置table_name
        table_query_op = TableQueryOp(
            name="TableQueryOp",
            attrs={
                "expression": "id>0 & nums>12",
                "inplace":False,
                "table_name": "new_table",
                "engine": "python",
                "reset_index": False,
            }
        )

        # 执行算子
        success = table_query_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        expression = self.attrs.get("expression")
        inplace = self.attrs.get("inplace", False)
        table_name = self.attrs.get("table_name")
        engine = self.attrs.get("engine", "python")
        reset_index = self.attrs.get("reset_index", True)

        new_df = in_table.query(expression, inplace=inplace, engine=engine)

        if inplace:
            if reset_index:
                in_table.reset_index(inplace=True, drop=True)
            op_context.output_tables.append(in_table)
        else:
            new_table = DataTable(name=table_name, data=new_df)
            if reset_index:
                new_table.reset_index(inplace=True, drop=True)
            op_context.output_tables.append(new_table)
        return True


op_register.register_op(TableQueryOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="expression", type="str", desc="查询表达式") \
    .add_attr(name="inplace", type="bool", desc="是否就地处理") \
    .add_attr(name="table_name", type="str", desc="新表名") \
    .add_attr(name="engine", type="str", desc="查询引擎") \
    .add_attr(name="reset_index", type="bool", desc="是否重置索引")
