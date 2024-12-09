from video_graph.common.proto.rpc_proto.video_script_pb2 import Op as OpPb
from video_graph.common.proto.rpc_proto.video_script_pb2 import VideoScriptRequest, ProtoBlobStoreKey, OpType, Caption, SubtitleOp
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddSubtitleOp(Op):
    """
    Function:
        增加字幕算子

    Attributes:
        request_column (str): 请求列名，默认为"request"。
        subtitle_group_column (str): 字幕组列名，默认为"subtitle_group"。
        normal_text_width (int): 普通文本宽度，默认为49。
        normal_text_height (int): 普通文本高度，默认为70。
        keyword_text_width (int): 关键字文本宽度，默认为70。
        keyword_text_height (int): 关键字文本高度，默认为100。
        subtitle_center_pos_width (int): 字幕中心位置宽度，默认为50。
        subtitle_center_pos_height (int): 字幕中心位置高度，默认为76。
        normal_font_size (int): 普通字体大小，默认为48。
        keyword_font_size (int): 关键字字体大小，默认为72。
        font_color (str): 字体颜色，默认为"#FFFFFF"。
        stroke_width (int): 描边宽度，默认为4。
        stroke_color (str): 描边颜色，默认为"#FF606B"。
        font (str): 字体，默认为"汉仪雅酷黑85W.ttf"。

    InputTables:
        timeline_table: 时间线表。
        shot_table: 镜号表。

    OutputTables:
        timeline_table: 添加了字幕的时间线表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_op/add_subtitle_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        timeline_table: DataTable = op_context.input_tables[1]
        request_column = self.attrs.get("request_column", "request")
        subtitle_group_column = self.attrs.get("subtitle_group_column", "subtitle_group_with_keyword")
        normal_text_width = self.attrs.get("normal_text_width", 49)
        normal_text_height = self.attrs.get("normal_text_height", 70)
        keyword_text_width = self.attrs.get("keyword_text_width", 70)
        keyword_text_height = self.attrs.get("keyword_text_height", 100)
        subtitle_center_pos_width = self.attrs.get("subtitle_center_pos_width", 50)
        subtitle_center_pos_height = self.attrs.get("subtitle_center_pos_height", 76)
        normal_font_size = self.attrs.get("normal_font_size", 48)
        keyword_font_size = self.attrs.get("keyword_font_size", 72)
        font_color = self.attrs.get("font_color", "#FFFFFF")
        stroke_width = self.attrs.get("stroke_width", 4)
        stroke_color = self.attrs.get("stroke_color", "#FF606B")
        font = self.attrs.get("font", "汉仪雅酷黑85W.ttf")

        request: VideoScriptRequest = timeline_table.loc[0, request_column]
        subtitle_group = shot_table.loc[0, subtitle_group_column]
        resolution = request.resolution
        normal_text_width_ratio = normal_text_width / resolution[0] * 100
        normal_text_height_ratio = normal_text_height / resolution[1] * 100
        keyword_text_width_ratio = keyword_text_width / resolution[0] * 100
        keyword_text_height_ratio = keyword_text_height / resolution[1] * 100

        subtitle_op = SubtitleOp()
        for start_time, end_time, items_list, keywords_list in subtitle_group:
            line_num = 0
            for one_line, keywords in zip(items_list, keywords_list):
                total_normal_text_num = sum([len(item) for item in one_line if item not in keywords])
                total_keyword_text_num = sum([len(item) for item in one_line if item in keywords])
                cur_start_pos = subtitle_center_pos_width - (total_normal_text_num / 2 * normal_text_width_ratio) - (
                        total_keyword_text_num / 2 * keyword_text_width_ratio)
                for one_item in one_line:
                    is_keyword = one_item in keywords
                    text_width = keyword_text_width if is_keyword else normal_text_width
                    text_height = keyword_text_height if is_keyword else normal_text_height
                    text_width_ratio = keyword_text_width_ratio if is_keyword else normal_text_width_ratio
                    text_height_ratio = keyword_text_height_ratio if total_keyword_text_num > 0 \
                        else normal_text_height_ratio
                    font_size = keyword_font_size if is_keyword else normal_font_size
                    text_len = len(one_item)
                    cur_end_pos = cur_start_pos + text_len * text_width_ratio
                    caption = Caption()
                    caption.text = one_item
                    caption.start_time = start_time
                    caption.end_time = end_time
                    caption.pos_w = (cur_start_pos + cur_end_pos) / 2
                    caption.pos_h = subtitle_center_pos_height + line_num * text_height_ratio
                    caption.font.CopyFrom(ProtoBlobStoreKey(db="ad", table="nieuwland-material", key=font))
                    caption.font_size = font_size
                    caption.is_key = is_keyword
                    caption.width = text_width * text_len
                    caption.height = text_height
                    caption.color = font_color
                    caption.stroke_width.append(stroke_width)
                    caption.stroke_color.append(stroke_color)
                    cur_start_pos = cur_end_pos
                    subtitle_op.text.append(caption)
                line_num += 1

        op_pb = OpPb()
        op_pb.op_type = OpType.SUBTITLE_OP_TYPE
        op_pb.subtitle_op.CopyFrom(subtitle_op)
        request.clip[-1].ops.append(op_pb)

        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddSubtitleOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="request_column", type="str", desc="request列名") \
    .add_attr(name="subtitle_group_column", type="str", desc="字幕列表列名") \
    .add_attr(name="normal_text_width", type="int", desc="普通字幕宽度") \
    .add_attr(name="normal_text_height", type="int", desc="普通字幕高度") \
    .add_attr(name="keyword_text_width", type="int", desc="关键字字幕宽度") \
    .add_attr(name="keyword_text_height", type="int", desc="关键字字幕高度") \
    .add_attr(name="subtitle_center_pos_width", type="int", desc="字幕中心位置宽度") \
    .add_attr(name="subtitle_center_pos_height", type="int", desc="字幕中心位置高度") \
    .add_attr(name="normal_font_size", type="int", desc="普通字幕字体大小") \
    .add_attr(name="keyword_font_size", type="int", desc="关键字字幕字体大小") \
    .add_attr(name="font_color", type="str", desc="字幕颜色") \
    .add_attr(name="stroke_width", type="int", desc="字幕描边宽度") \
    .add_attr(name="stroke_color", type="str", desc="字幕描边颜色") \
    .add_attr(name="font", type="str", desc="字幕字体")
