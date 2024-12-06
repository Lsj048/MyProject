import random

from video_graph import logger
from video_graph.common.client.gpt4o_client import extract_title_from_asr
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class ExtractTitleFromSubtitleOp(Op):
    """
    Function:
        从台词中提取标题，通过 gpt4o 实现

    Attributes:
        subtitle_column (str): 字幕列名，默认为 "subtitle"。
        title_column (str): 标题列名，默认为 "title"。
        default_title (list): 默认兜底标题，默认为 ["好看到停不下来", "超爽虐文一口气看完"]。

    InputTables:
        shot_table: 输入表格。

    OutputTables:
        shot_table: 输出表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/extract_title_from_subtitle_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.text_process_op.extract_title_from_subtitle_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                "subtitle": ["落尘仙域的天空，似乎在这一瞬间被撕裂，原本的宁静被打破。一个强大而又熟悉的身影矗立在天地之间，周身散发着无与伦比的威压，\
                            让围攻的众仙帝们不由自主地后退，心中充满了恐惧与不安。那是一个被认为已经陨落在纪元战场的强者，如今却以巅峰仙帝的姿态重现于世，\
                            仿佛是从神话中归来的传奇。 “他居然已经到了巅峰仙帝境界！”围攻的仙帝们面面相觑，目光中充满了不可思议与震惊。\
                            那种感觉就像是面对着一头沉睡的巨兽，随时都有可能苏醒，带来毁灭的灾难。后背的冷汗顺着脊椎滑落，每个人的心中都涌起了强烈的危机感，\
                            仿佛整个世界都在这一刻变得不再安全。"]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        extract_title_from_subtitle_op = ExtractTitleFromSubtitleOp(
            name="ExtractTitleFromSubtitleOp",
            attrs={
               "subtitle_column": "subtitle",
                "title_column": "title",
                "default_title":"default_title"
            }
        )

        # 执行算子
        success = extract_title_from_subtitle_op.process(op_context)

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
        title_column = self.attrs.get("title_column", "title")
        default_title = self.attrs.get("default_title", ["好看到停不下来", "超爽虐文一口气看完"])

        shot_table[title_column] = None
        for index, row in shot_table.iterrows():
            subtitle = row.get(subtitle_column)
            title = None
            if subtitle:
                title = extract_title_from_asr(subtitle)

            if not title:
                title = random.choice(default_title)
                logger.info(f"gpt extract title from {subtitle} failed, using default title: {title}")
            shot_table.at[index, title_column] = title

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(ExtractTitleFromSubtitleOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="subtitle_column", type="str", desc="字幕列名") \
    .add_attr(name="title_column", type="str", desc="标题列名") \
    .add_attr(name="default_title", type="list", desc="默认兜底标题")
