import random

from video_graph import logger
from video_graph.common.utils.tools import extract_title_from_ocr
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class ExtractTitleFromOcrOp(Op):
    """
    Function:
        从 ocr 中提取标题

    Attributes:
        video_ocr_column (str): 视频 ocr 列名，默认为 "ocr"。
        title_column (str): 标题列名，默认为 "title"。
        default_title (List[str]): 默认标题，默认为 ["好看到停不下来", "超爽虐文一口气看完"]。

    InputTables:
        shot_table: 输入表格。

    OutputTables:
        shot_table: 输出表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/extract_title_from_ocr_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.text_process_op.extract_title_from_ocr_op import *

# 创建输入表格，ocr信息来源于PhotoOcrDetectMmuOp算子，下列仅为示例
input_table = DataTable(
    name="TestTable",
    data = {
        "video_ocr":[{"message": 'SUCCESS',"mmuVideoOcrResult": [{"textTrack": [{"textType": 1,"textProb": 0.8,"text": "为了测试的文本","frameTracks": [{"framePostfix": 0,"points": [{"x": 100, "y": 200},{"x": 300, "y": 200},{"x": 300, "y": 400},{"x": 100, "y": 400}]}]}]}]}]
    }
)
# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(input_table)

# 配置并实例化算子
extract_title_from_ocr_op = ExtractTitleFromOcrOp(
    name="ExtractTitleFromOcrOp",
    attrs={
        "video_ocr_column":"video_ocr",
        "title_column":"title",
        "default_title":["好看到停不下来", "超爽虐文一口气看完"]
    }
)

# 执行算子
success = extract_title_from_ocr_op.process(op_context)

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
        video_ocr_column = self.attrs.get("video_ocr_column", "ocr")
        title_column = self.attrs.get("title_column", "title")
        default_title = self.attrs.get("default_title", ["好看到停不下来", "超爽虐文一口气看完"])

        shot_table[title_column] = None
        for index, row in shot_table.iterrows():
            video_ocr = row.get(video_ocr_column)
            title = None
            if video_ocr:
                title = extract_title_from_ocr(video_ocr)

            if not title:
                title = random.choice(default_title)
                logger.info(f"extract title from {video_ocr} failed, using default title: {title}")
            shot_table.at[index, title_column] = title

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(ExtractTitleFromOcrOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="video_ocr_column", type="str", desc="视频ocr列名") \
    .add_attr(name="title_column", type="str", desc="标题列名") \
    .add_attr(name="default_title", type="list", desc="默认标题")
