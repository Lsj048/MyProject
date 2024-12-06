import json

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class TextVideoMatchPrepareOp(Op):
    """
    Function:
        文本视频匹配数据准备算子，准备切片相关的信息

    Attributes:
        video_shot_clip_column (str): 视频切片列名，默认为"shot_clip"
        clip_resource_info_column (str): 切片资源信息列名，默认为"clip_resource_info"
        shot_type_column (str): 镜号类型列名，默认为"shot_type"
        video_index_column (str): 视频索引列名，默认为"video_index"
        width_column (str): 视频宽度列名，默认为"width"
        height_column (str): 视频高度列名，默认为"height"
        shot_clip_num_column (str): 镜号切片数列名，默认为"shot_clip_num"
        shot_clip_ocr_info_column (str): 镜号切片OCR信息列名，默认为"shot_clip_ocr"
        shot_clip_index_column (str): 镜号切片索引列名，默认为"shot_clip_index"
        shot_clip_valid_subtitle_region_column (str): 镜号切片有效字幕区域列名，默认为"shot_clip_valid_subtitle_region"
        video_end_clip_num_th (int): 视频结束切片数阈值，用于判断是否标记开始和结束切片，默认为3

    InputTables:
        material_table: 素材表
        shot_table: 镜号表

    OutputTables:
        shot_table: 添加了切片资源信息的镜号表

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_match_op/text_video_match_prepare_op.py?ref_type=heads

    Examples:
        #等待作者更新～
    """

    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        shot_table: DataTable = op_context.input_tables[1]
        video_shot_clip_column = self.attrs.get("video_shot_clip_column", "shot_clip")
        clip_resource_info_column = self.attrs.get("clip_resource_info_column", "clip_resource_info")
        shot_type_column = self.attrs.get("shot_type_column", "shot_type")
        video_index_column = self.attrs.get("video_index_column", "video_index")
        width_column = self.attrs.get("width_column", "width")
        height_column = self.attrs.get("height_column", "height")
        shot_clip_num_column = self.attrs.get("shot_clip_num_column", "shot_clip_num")
        shot_clip_ocr_info_column = self.attrs.get("shot_clip_ocr_info_column", "shot_clip_ocr")
        shot_clip_index_column = self.attrs.get("shot_clip_index_column", "shot_clip_index")
        shot_clip_valid_subtitle_region_column = self.attrs.get("shot_clip_valid_subtitle_region_column",
                                                                "shot_clip_valid_subtitle_region")
        video_end_clip_num_th = self.attrs.get("video_end_clip_num_th", 3)

        clip_resource_info = []
        for material_index, material_row in material_table.iterrows():
            video_index = material_row.get(video_index_column)
            width = material_row.get(width_column)
            height = material_row.get(height_column)
            size = (width, height)
            shot_clip = material_row.get(video_shot_clip_column)
            shot_clip_num = material_row.get(shot_clip_num_column)
            shot_clip_ocr_info = material_row.get(shot_clip_ocr_info_column)
            shot_clip_index = material_row.get(shot_clip_index_column)
            shot_clip_valid_subtitle_region = material_row.get(shot_clip_valid_subtitle_region_column)

            is_begin_clip = (shot_clip_index == 1 and shot_clip_num > video_end_clip_num_th)
            is_end_clip = (shot_clip_index == shot_clip_num and shot_clip_num > video_end_clip_num_th)
            valid_region = shot_clip_valid_subtitle_region['valid_region'] if shot_clip_valid_subtitle_region \
                else [{"start_time": shot_clip[0], "end_time": shot_clip[1], "bbox": [0, 0, width, height]}]
            shot_clip_ocr_json = json.dumps(shot_clip_ocr_info) if shot_clip_ocr_info else '[]'

            clip_resource_info.append((video_index, shot_clip[2], shot_clip[0], shot_clip[1],
                                       valid_region, size, is_end_clip, is_begin_clip, shot_clip_ocr_json))

        shot_table[clip_resource_info_column] = None
        for shot_index, shot_row in shot_table.iterrows():
            if shot_row.get(shot_type_column) != "montage":
                continue

            shot_table.at[shot_index, clip_resource_info_column] = clip_resource_info

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(TextVideoMatchPrepareOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="video_shot_clip_column", type="str", desc="视频切镜结果列名") \
    .add_attr(name="clip_resource_info_column", type="str", desc="切镜聚合结果列名") \
    .add_attr(name="shot_type_column", type="str", desc="镜号类型列名") \
    .add_attr(name="video_index_column", type="str", desc="视频编号列名") \
    .add_attr(name="width_column", type="str", desc="视频宽度列名") \
    .add_attr(name="height_column", type="str", desc="视频高度列名") \
    .add_attr(name="shot_clip_num_column", type="str", desc="镜号切片数量列名") \
    .add_attr(name="shot_clip_subtitle_list_column", type="str", desc="镜号切片字幕列表列名") \
    .add_attr(name="shot_clip_index_column", type="str", desc="镜号切片索引列名") \
    .add_attr(name="shot_clip_valid_subtitle_region_column", type="str", desc="镜号切片有效字幕区域列名") \
    .add_attr(name="video_end_clip_num_th", type="int", desc="视频结束镜号数量阈值")
