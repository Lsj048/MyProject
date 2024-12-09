from video_graph.common.client.gpt4o_client import correct_asr
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class CorrectTextByGpt4oOp(Op):
    """
    Function:
        基于 gpt4o 纠正文本

    Attributes:
        text_column (str): 文本列名，默认为 "text"。
        corrected_text_column (str): 纠正后文本列名，默认为 "corrected_text"。

    InputTables:
        shot_table: 输入表格。

    OutputTables:
        shot_table: 输出表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/correct_text_by_gpt4o_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.text_process_op.correct_text_by_gpt4o_op import *

# 创建输入表格
input_table = DataTable(
    name="TestTable",
    data = {
        "origin_text": ["这是一段不太顺通有搓别字的de台词而且作者写得又臭又长需要简短浓缩铀话"]
    }
)

# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(input_table)

# 配置并实例化算子
correct_text_by_gpt4o_op = CorrectTextByGpt4oOp(
    name="ConvertScriptToMultiShotOp",
    attrs={
       "text_column": "origin_text",
        "corrected_text_column": "better_text"
    }
)

# 执行算子
success = correct_text_by_gpt4o_op.process(op_context)

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
        text_column = self.attrs.get("text_column", "text")
        corrected_text_column = self.attrs.get("corrected_text_column", "corrected_text")

        status = True
        shot_table[corrected_text_column] = None
        for index, row in shot_table.iterrows():
            text = row.get(text_column)
            if text:
                corrected_text = correct_asr(text)
                if not corrected_text:
                    status = False
                    continue
                shot_table.at[index, corrected_text_column] = corrected_text

        if status is False:
            self.fail_reason = "gpt文本纠正失败"
            self.trace_log.update({"fail_reason": self.fail_reason})
            return False

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(CorrectTextByGpt4oOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="text_column", type="str", desc="文本列名") \
    .add_attr(name="corrected_text_column", type="str", desc="纠错后文本列名")
