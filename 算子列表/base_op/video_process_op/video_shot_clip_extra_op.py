import copy
from collections import defaultdict

import numpy as np

from video_graph.common.utils.kconf import get_kconf_value
from video_graph.common.utils.tools import merge_time
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoShotClipExtraOp(Op):
    """
    Function:
        视频切镜信息补充算子，将视频的OCR信息和字幕信息拆分到切片粒度，并判断切片是否有效
    TODO: 算子可拆分为多个子算子

    Attributes:
        video_blob_key_column (str): 视频BlobKey所在的列名，默认为"video_blob_key"
        video_shot_clip_column (str): 视频切片列名，默认为"shot_clip"
        video_ocr_info_column (str): 视频OCR信息列名，默认为"ocr"
        video_subtitle_list_column (str): 视频字幕列名，默认为"subtitle_list"
        shot_clip_ocr_info_column (str): 切片OCR信息列名，默认为"shot_clip_ocr"
        shot_clip_subtitle_list_column (str): 切片字幕列名，默认为"shot_clip_subtitle_list"
        shot_clip_index_column (str): 切片索引列名，默认为"shot_clip_index"
        shot_clip_valid_subtitle_region_column (str): 切片有效字幕区域列名，默认为"shot_clip_valid_subtitle_region"
        duration_column (str): 视频时长列名，默认为"duration"
        width_column (str): 视频宽度列名，默认为"width"
        height_column (str): 视频高度列名，默认为"height"

    InputTables:
        material_table: 视频BlobKey所在的表格

    OutputTables:
        material_table: 添加了视频切片补充信息的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_process_op/video_shot_clip_extra_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        video_blob_key_column = self.attrs.get("video_blob_key_column", "video_blob_key")
        video_shot_clip_column = self.attrs.get("video_shot_clip_column", "shot_clip")
        video_ocr_info_column = self.attrs.get("video_ocr_info_column", "ocr")
        video_subtitle_list_column = self.attrs.get("video_subtitle_list_column", "subtitle_list")
        shot_clip_ocr_info_column = self.attrs.get("shot_clip_ocr_info_column", "shot_clip_ocr")
        shot_clip_subtitle_list_column = self.attrs.get("shot_clip_subtitle_list_column", "shot_clip_subtitle_list")
        shot_clip_invalid_reason_column = self.attrs.get("shot_clip_invalid_reason_column", "invalid_reason")
        shot_clip_index_column = self.attrs.get("shot_clip_index_column", "shot_clip_index")
        shot_clip_valid_subtitle_region_column = self.attrs.get("shot_clip_valid_subtitle_region_column",
                                                                "shot_clip_valid_subtitle_region")
        #duration_column = self.attrs.get("duration_column", "duration")
        width_column = self.attrs.get("width_column", "width")
        height_column = self.attrs.get("height_column", "height")
        shot_clip_valid_clip_column = self.attrs.get("shot_clip_valid_clip_column", "shot_clip_valid_clip")
        center_text_width_iou_th = self.attrs.get("center_text_width_iou_th", 1.0)
        center_text_height_iou_th = self.attrs.get("center_text_height_iou_th", 0.1)

        kconf_params: dict = get_kconf_value("ad.algorithm.nieuwlandGeneration", "json")
        mask_subtitle_cfg: dict = kconf_params['mask_subtitle_cfg']
        caption_upper_bound_range = mask_subtitle_cfg["caption_upper_bound_range"]
        subtitle_time_range_th = mask_subtitle_cfg["subtitle_time_range_th"]
        text_width_th, text_height_th = mask_subtitle_cfg["subtitle_pixel_th"]
        text_width_range, text_height_range = mask_subtitle_cfg["subtitle_center_range"]
        subtitle_center_type = mask_subtitle_cfg["subtitle_center_type"]
        filter_ocr_area_th_rel = mask_subtitle_cfg["filter_ocr_area_th_rel"]
        filter_subtitle_area_th_rel = mask_subtitle_cfg["filter_subtitle_area_th_rel"]

        valid_clip_num = 0
        shot_clip_counter = defaultdict(int)
        material_table[shot_clip_index_column] = None
        material_table[shot_clip_ocr_info_column] = None
        material_table[shot_clip_subtitle_list_column] = None
        material_table[shot_clip_valid_subtitle_region_column] = None
        material_table[shot_clip_valid_clip_column] = True
        material_table[shot_clip_invalid_reason_column] = None
        for index, row in material_table.iterrows():
            valid_clip = True
            invalid_reason = None
            shot_clip = row.get(video_shot_clip_column)
            if shot_clip is None or str(shot_clip) == 'nan' or len(shot_clip) != 3:
                continue

            #duration = row.get(duration_column)
            width = row.get(width_column)
            height = row.get(height_column)
            video_blob_key = row.get(video_blob_key_column)

            clip_start_time = shot_clip[0] / 1000.0
            clip_end_time = shot_clip[1] / 1000.0

            # shot_clip_index
            shot_clip_counter[video_blob_key] += 1
            material_table.at[index, shot_clip_index_column] = shot_clip_counter[video_blob_key]

            # shot_clip_ocr_info
            ocr_info = row.get(video_ocr_info_column)
            if ocr_info is None:
                continue
            shot_clip_ocr_list = []
            for ocr in ocr_info:
                start_time_union = max(ocr["start_time"], clip_start_time)
                end_time_union = min(ocr["end_time"], clip_end_time)
                if (end_time_union - start_time_union) > 0.1:
                    shot_clip_ocr_list.append(copy.deepcopy(ocr))
                    shot_clip_ocr_list[-1]["start_time"] = start_time_union
                    shot_clip_ocr_list[-1]["end_time"] = end_time_union

            # shot_clip_subtitle_list
            subtitle_list = row.get(video_subtitle_list_column)
            if subtitle_list is None:
                continue
            shot_clip_subtitle_list = []
            for subtitle in subtitle_list:
                start_time_union = max(subtitle["start_time"], clip_start_time)
                end_time_union = min(subtitle["end_time"], clip_end_time)
                if (end_time_union - start_time_union) > 0.1:
                    shot_clip_subtitle_list.append(copy.deepcopy(subtitle))
                    shot_clip_subtitle_list[-1]["start_time"] = start_time_union
                    shot_clip_subtitle_list[-1]["end_time"] = end_time_union

            material_table.at[index, shot_clip_ocr_info_column] = shot_clip_ocr_list
            material_table.at[index, shot_clip_subtitle_list_column] = shot_clip_subtitle_list

            # filter big text
            subtitle_height_list = [item["bbox"][3] - item["bbox"][1] for item in shot_clip_subtitle_list]
            if subtitle_height_list and max(subtitle_height_list) >= text_height_th:
                valid_clip = False
                invalid_reason = "big_text"

            # filter center text
            center_subtitle_list = []
            for item in shot_clip_subtitle_list:
                op_context.perf_ctx("shot_clip_text_type_count", extra1=str(item["textType"]))
                if item["textType"] not in subtitle_center_type:
                    continue

                width_range = [item["bbox"][0] / width, item["bbox"][2] / width]
                height_range = [item["bbox"][1] / height, item["bbox"][3] / height]

                width_inter = min(text_width_range[1], width_range[1]) - max(text_width_range[0], width_range[0])
                width_union = abs(text_width_range[1] - text_width_range[0]) + 1e-8
                width_iou = width_inter / width_union

                height_inter = min(text_height_range[1], height_range[1]) - max(text_height_range[0], height_range[0])
                height_union = abs(text_height_range[1] - text_height_range[0]) + 1e-8
                height_iou = height_inter / height_union

                if width_iou > center_text_width_iou_th or height_iou >= center_text_height_iou_th:
                    center_subtitle_list.append(item)

            if center_subtitle_list:
                valid_clip = False
                invalid_reason = "center_text"

            # filter overmuch text
            video_screen = np.zeros((width, height))
            for item in shot_clip_ocr_list:
                x1, y1, x2, y2 = int(item["bbox"][0]), int(item["bbox"][1]), int(item["bbox"][2]), int(item["bbox"][3])
                video_screen[y1:y2, x1:x2] = 1
            ocr_area = (video_screen.sum()) / (width * height + 1e-8)
            if ocr_area >= filter_ocr_area_th_rel:
                valid_clip = False
                invalid_reason = "overmuch_text"

            # filter overmuch subtitle
            video_screen = np.zeros(height)
            for item in shot_clip_subtitle_list:
                x1, y1, x2, y2 = int(item["bbox"][0]), int(item["bbox"][1]), int(item["bbox"][2]), int(item["bbox"][3])
                video_screen[y1:y2] = 1
            ocr_area = (video_screen.sum()) / (height + 1e-8)
            if ocr_area >= filter_subtitle_area_th_rel:
                valid_clip = False
                invalid_reason = "overmuch_subtitle"

            # shot_clip_valid_subtitle_region
            subtitles_upper = max([item["bbox"][1] for item in shot_clip_subtitle_list]) \
                if shot_clip_subtitle_list else None
            subtitle_time_range = sum([(item[1] - item[0]) for item in merge_time(shot_clip_subtitle_list)])
            subtitle_time_iou = abs(subtitle_time_range) / (abs(clip_end_time - clip_start_time) + 1e-8)
            if shot_clip_subtitle_list and subtitle_time_iou > subtitle_time_range_th and (
                    not caption_upper_bound_range or subtitles_upper <= caption_upper_bound_range[0]):
                shot_clip_valid_subtitle_region = {"valid_clip": True, "is_subtitle": True,
                                                   "valid_region": shot_clip_subtitle_list}
            else:
                shot_clip_valid_subtitle_region = {"valid_clip": True, "is_subtitle": False,
                                                   "valid_region": [{"start_time": clip_start_time,
                                                                     "end_time": clip_end_time,
                                                                     "bbox": [0, 0, width, height]}]}

            if not valid_clip:
                op_context.perf_ctx("invalid_shot_clip_count", extra1=invalid_reason)
            else:
                valid_clip_num += 1

            material_table.at[index, shot_clip_valid_clip_column] = valid_clip
            material_table.at[index, shot_clip_invalid_reason_column] = invalid_reason
            material_table.at[index, shot_clip_valid_subtitle_region_column] = shot_clip_valid_subtitle_region

        op_context.perf_ctx("valid_shot_clip_num", micros=valid_clip_num)
        op_context.output_tables.append(material_table)
        return True


op_register.register_op(VideoShotClipExtraOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="video_shot_clip_column", type="str", desc="切镜信息列名") \
    .add_attr(name="video_ocr_info_column", type="str", desc="ocr信息列名") \
    .add_attr(name="video_subtitle_list_column", type="str", desc="字幕位置信息列名") \
    .add_attr(name="shot_clip_ocr_info_column", type="str", desc="切镜ocr信息列名") \
    .add_attr(name="shot_clip_subtitle_list_column", type="str", desc="切镜字幕位置信息列名") \
    .add_attr(name="shot_clip_index_column", type="str", desc="切镜索引列名") \
    .add_attr(name="shot_clip_valid_subtitle_region_column", type="str", desc="切镜有效字幕区域列名") \
    .add_attr(name="duration_column", type="str", desc="视频时长列名") \
    .add_attr(name="width_column", type="str", desc="视频宽度列名") \
    .add_attr(name="height_column", type="str", desc="视频高度列名")
