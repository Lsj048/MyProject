from video_graph.common.client.gpt4o_client import get_novel_subtitle
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class GenerateSubtitleByGpt4oOp(Op):
    """
    Function:
        通过 gpt4o 生成台词脚本

    Attributes:
        subtitle_column (str): 台词列名，默认为 "subtitle"。

    InputTables:
        shot_table: 输入表格。

    OutputTables:
        shot_table: 输出表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/generate_subtitle_by_gpt4o_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.text_process_op.generate_subtitle_by_gpt4o_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                "shot_id": [1],
                "scene_description": ["Scene 1"]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        generate_subtitle_by_gpt4o_op = GenerateSubtitleByGpt4oOp(
            name="GenerateSubtitleByGpt4oOp",
            attrs={
               "subtitle_column": "subtitle",
            }
        )

        # 执行算子
        success = generate_subtitle_by_gpt4o_op.process(op_context)

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
        subtitle_column = self.attrs.get("subtitle_column", "subtitle")

        status = True
        shot_table[subtitle_column] = None
        for index, row in shot_table.iterrows():
            subtitle = get_novel_subtitle()
            if not subtitle:
                status = False
                continue
            shot_table.at[index, subtitle_column] = subtitle

        if status is False:
            self.fail_reason = "gpt生成台词失败"
            self.trace_log.update({"fail_reason": self.fail_reason})
            return False

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(GenerateSubtitleByGpt4oOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="subtitle_column", type="str", desc="字幕列名")
