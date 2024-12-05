import jsonpath
from google.protobuf.json_format import MessageToDict

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class ExtractColumnFromPBOp(Op):
    """
    【local】从 Protobuf 列中提取列算子，将 Protobuf 列转换为字典后用 jsonpath 匹配方式提取

    Attributes:
        pb_column (str): Protobuf 列名。
        extracted_column (str): 要提取的列名，支持 jsonpath 匹配方式。
        alias_column_name (str, optional): 提取后的列名，默认为提取的列名。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        in_table: 加入新列后的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/extract_column_from_pb_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.extract_column_from_pb_op import *
        from test_op.test_pb2 import TestMessage

        # 创建输入表格，其中：
        #test_pb2是自定义的test.proto文件编译生成的文件
        #TestMessage是test.proto文件中定义的消息名称
        input_table = DataTable(
            name="TestTable",
            data = {
                'pb_column': [TestMessage(name="张三", age=13, address="北京")
                              ,TestMessage(name="李四", age=17, address="上海")]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        extract_column_from_pb_op = ExtractColumnFromPBOp(
            name="ExtractColumnFromPBOp",
            attrs={
                "pb_column": "pb_column",
                "extracted_column": "$.address",
                "alias_column_name": "extracted_name"
            }
        )

        # 执行算子
        success = extract_column_from_pb_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        pb_column = self.attrs.get("pb_column")
        extracted_column = self.attrs.get("extracted_column")
        alias_column_name = self.attrs.get("alias_column_name", extracted_column.split('.')[-1])

        if pb_column not in in_table.columns:
            return False

        in_table[alias_column_name] = None
        for index, row in in_table.iterrows():
            pb_value = row.get(pb_column)
            if not pb_value:
                continue
            dict_value = MessageToDict(pb_value)
            extracted_values = jsonpath.jsonpath(dict_value, extracted_column)
            if extracted_values and len(extracted_values) > 0:
                in_table.at[index, alias_column_name] = extracted_values[0]

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(ExtractColumnFromPBOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="pb_column", type="str", desc="pb列名") \
    .add_attr(name="extracted_column", type="str", desc="提取的列名") \
    .add_attr(name="alias_column_name", type="str", desc="新列别名")

