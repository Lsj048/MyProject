from video_graph.common.client.gpt4o_client import translate_ch2id, translate_id2ch, translate_ch2pt, translate_pt2ch, \
    translate_en2ch, translate_ch2en
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class TranslateTextOp(Op):
    """
    【remote】翻译文本为指定语言，支持以下几种翻译：
    * 葡萄牙语(pt) <-> 中文(ch)
    * 印尼语(id) <-> 中文(ch)
    * 英语(en) <-> 中文(ch)

    Attributes:
        text_column (str): 待翻译的文本列名，默认为"text"。
        translated_text_column (str): 翻译后的文本列名，默认为"translated_text"。
        source_language (str): 文本语言，默认为"ch"。
        target_language (str): 目标文本语言，默认为"id"。

    InputTables:
        text_table: 输入表格。

    OutputTables:
        text_table: 输出表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/translate_text_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.text_process_op.translate_text_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                "text": ["hello", "world", "banana"]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        translate_text_op = TranslateTextOp(
            name="TranslateTextOp",
            attrs={
                "text_column":"text",
                "translated_text_column":"translated_text",
                "source_language":"en",
                "target_language":"ch"
            }
        )

        # 执行算子
        success = translate_text_op.process(op_context)

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
        text_table: DataTable = op_context.input_tables[0]
        text_column = self.attrs.get("text_column", "text")
        translated_text_column = self.attrs.get("translated_text_column", "translated_text")
        source_language = self.attrs.get("source_language", "ch")
        target_language = self.attrs.get("target_language", "id")

        translate_func = None
        if source_language == "ch" and target_language == "id":
            translate_func = translate_ch2id
        elif source_language == "id" and target_language == "ch":
            translate_func = translate_id2ch
        elif source_language == "ch" and target_language == "pt":
            translate_func = translate_ch2pt
        elif source_language == "pt" and target_language == "ch":
            translate_func = translate_pt2ch
        elif source_language == "en" and target_language == "ch":
            translate_func = translate_en2ch
        elif source_language == "ch" and target_language == "en":
            translate_func = translate_ch2en

        if translate_func is None:
            self.fail_reason = f"不支持的翻译语言：{source_language}2{target_language}"
            self.trace_log.update({"fail_reason": self.fail_reason})
            return False

        status = True
        text_table[translated_text_column] = None
        for index, row in text_table.iterrows():
            text = row.get(text_column)
            if text is None:
                status = False
                continue

            translate_text = translate_func(text)
            if not translate_text:
                status = False
                continue
            text_table.at[index, translated_text_column] = translate_text

        if status is False:
            self.fail_reason = f"文本翻译失败：{source_language}2{target_language}"
            self.trace_log.update({"fail_reason": self.fail_reason})
            return False

        op_context.output_tables.append(text_table)
        return True


op_register.register_op(TranslateTextOp) \
    .add_input(name="text_table", type="DataTable", desc="镜号表") \
    .add_output(name="text_table", type="DataTable", desc="镜号表") \
    .add_attr(name="text_column", type="str", desc="待翻译的文本列名") \
    .add_attr(name="translated_text_column", type="str", desc="翻译后的文本列名") \
    .add_attr(name="source_language", type="str", desc="文本语言") \
    .add_attr(name="target_language", type="str", desc="目标文本语言")
