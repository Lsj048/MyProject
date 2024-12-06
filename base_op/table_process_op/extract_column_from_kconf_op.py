import time

import jsonpath

from video_graph.common.utils.kconf import get_kconf_value
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class ExtractColumnFromKconfOp(Op):
    """
    Function:
        从 KConf 列中提取列算子，支持 json 和 tail_number 类型的 KConf

    Attributes:
        kconf_key_name (str): KConf Key 名称。
        kconf_value_type (str): KConf Value 类型，可选值为 "json" 或 "tail_number"。
        json_extracted_column (str): 要提取的列名，支持 jsonpath 匹配方式。
        tail_number_random_column (str, optional): 随机数列名，用于 tail_number 类型的 KConf，默认为 None。
        tail_number_use_timestamp (bool, optional): 是否使用时间戳作为随机数，默认为 False。
        alias_column_name (str, optional): 提取后的列名，默认为提取的列名。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        in_table: 加入新列后的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/extract_column_from_kconf_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.extract_column_from_kconf_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                "id": [1, 2, 3]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子，其中kconf_key_name、kconf_value_type、json_extracted_column都需要真实可用，本例中仅为演示
        extract_column_from_fconf_op = ExtractColumnFromKconfOp(
            name="ExtractColumnFromKconfOp",
            attrs={
                "kconf_key_name": "example_key",
                "kconf_value_type": "json",
                "json_extracted_column": "$.id"
            }
        )

        # 执行算子
        success = extract_column_from_fconf_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        kconf_key_name = self.attrs.get("kconf_key_name")
        kconf_value_type = self.attrs.get("kconf_value_type")
        json_extracted_column = self.attrs.get("json_extracted_column")
        tail_number_random_column = self.attrs.get("tail_number_random_column")
        tail_number_use_timestamp = self.attrs.get("tail_number_use_timestamp", False)
        alias_column_name = self.attrs.get("alias_column_name",
                                           f"{kconf_key_name.split('.')[-1]}_{json_extracted_column.split('.')[-1]}")

        kconf_value = get_kconf_value(kconf_key_name, kconf_value_type)
        if kconf_value_type == "json" and json_extracted_column:
            target_values = jsonpath.jsonpath(kconf_value, json_extracted_column)
            target_value = target_values[-1] if target_values and len(target_values) > 0 else None
        elif kconf_value_type == "tail_number":
            tail_number_random = 0
            if tail_number_use_timestamp:
                tail_number_random = int(time.time() * 1000)
            elif tail_number_random_column and tail_number_random_column in in_table.columns:
                tail_number_random = in_table.loc[0, tail_number_random_column]
            target_value = kconf_value.is_for_on(tail_number_random)
        else:
            target_value = kconf_value

        in_table[alias_column_name] = [target_value] * in_table.shape[0]

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(ExtractColumnFromKconfOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="kconf_key_name", type="str", desc="kconf key 名字") \
    .add_attr(name="kconf_value_type", type="str", desc="kconf 值类型") \
    .add_attr(name="json_extracted_column", type="str", desc="json类型提取的列名") \
    .add_attr(name="tail_number_random_column", type="str", desc="尾号随机提取的列名") \
    .add_attr(name="tail_number_use_timestamp", type="bool", desc="尾号是否使用时间戳") \
    .add_attr(name="alias_column_name", type="str", desc="新列别名")

