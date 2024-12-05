from video_graph.common.client.gpt4o_client import convert_script_to_multi_shot
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class ConvertScriptToMultiShotOp(Op):
    """
    【remote】单镜脚本升级为多镜，通过 gpt4o 实现

    Attributes:
        single_script_column (str): 单镜脚本列名，默认为 "single_script"。
        multi_script_column (str): 多镜脚本列名，默认为 "multi_script"。

    InputTables:
        shot_table: 输入表格。

    OutputTables:
        shot_table: 输出表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/convert_script_to_multishot_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.text_process_op.convert_script_to_multishot_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                "single_script": ["还在纠结上哪儿买？上拼多多！百万好物任你挑！"]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        convert_script_to_multishot_op = ConvertScriptToMultiShotOp(
            name="ConvertScriptToMultiShotOp",
            attrs={
               "single_script_column": "single_script",
                "multi_script_column": "multi_script"
            }
        )

        # 执行算子
        success = convert_script_to_multishot_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        single_script_column = self.attrs.get("single_script_column", "single_script")
        multi_script_column = self.attrs.get("multi_script_column", "multi_script")

        status = True
        shot_table[multi_script_column] = None
        for index, row in shot_table.iterrows():
            single_script = row.get(single_script_column)
            if isinstance(single_script, list):
                single_script = ''.join(single_script)
            multi_script = convert_script_to_multi_shot(single_script)
            if not multi_script:
                status = False
                continue
            shot_table.at[index, multi_script_column] = multi_script

        if status is False:
            self.fail_reason = "gpt脚本转换失败"
            self.trace_log.update({"fail_reason": self.fail_reason})
            return False

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(ConvertScriptToMultiShotOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="single_script_column", type="str", desc="单镜脚本列名") \
    .add_attr(name="multi_script_column", type="str", desc="多镜脚本列名")
