import json
import os

import numpy as np
from video_element_detection.utils import extract_subtitle

from video_graph.common.client.client_manager import ClientManager
from video_graph.common.utils.blobstore import BlobStoreClientManager
from video_graph.common.utils.kconf import get_kconf_value
from video_graph.common.utils.logger import logger
from video_graph.common.utils.tools import parse_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoOcrDetectOp(Op):
    """
    Function:
        视频OCR检测算子，输出ocr结果和subtitle信息

    Attributes:
        video_blob_key_column (str): 视频BlobKey所在的列名，默认为"video_blob_key"
        video_ocr_res_column (str): 视频OCR结果列名，默认为"ocr"
        video_subtitle_column (str): 视频字幕列名，默认为"subtitle_list"
        video_ocr_cover_ratio_column (str): 视频OCR覆盖比例列名，默认为"ocr_cover_ratio"
        width_column (str): 视频宽度列名，默认为"width"
        height_column (str): 视频高度列名，默认为"height"
        ocr_cache_key_template (str): OCR缓存Key模板，默认为"video_element_detection_{}_{}.json"
        valid_video_column (str): 判断视频是否有效的列名，默认为"valid_video"
        duration_column (str): 视频时长列名，默认为"duration"

    InputTables:
        material_table: 视频BlobKey所在的表格

    OutputTables:
        material_table: 添加了视频OCR信息的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_process_op/video_ocr_detect_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.video_process_op.video_ocr_detect_op import *

# 创建输入表格，视频信息来源于VideoBaseInfoOp算子，blob_key通过FileUploaderOp算子获得
input_table = DataTable(
    name="TestTable",
    data = {
        "video_blob_key": ["ad_nieuwland-material_test_video.mp4"],
        "width": [720],
        "height": [1280],
        "duration": [4.723644],
        "valid_video": [True]
    }
)

# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(input_table)

# 配置并实例化算子，其中redis_cluster必须是真实存在的集群
video_ocr_detect_op = VideoOcrDetectOp(
    name="video_ocr_detect_op",
    attrs={
        "video_blob_key_column": "video_blob_key",
        "video_ocr_res_column": "ocr_result",
        "video_subtitle_column": "subtitle_list",
        "video_ocr_cover_ratio_column": "ocr_cover_ratio",
        "width_column": "width",
        "height_column": "height",
        "ocr_cache_key_template": "video_element_detection_{}_{}.json",
        "valid_video_column": "valid_video",
        "duration_column": "duration"
    }
)

# 执行算子
success = video_ocr_detect_op.process(op_context)

# 检查输出表格
if success:
    output_table = op_context.output_tables[0]
    display(output_table)
else:
    print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        video_blob_key_column = self.attrs.get("video_blob_key_column", "video_blob_key")
        video_ocr_res_column = self.attrs.get("video_ocr_res_column", "ocr")
        video_subtitle_list_column = self.attrs.get("video_subtitle_column", "subtitle_list")
        video_ocr_cover_ratio_column = self.attrs.get("video_ocr_cover_ratio_column", "ocr_cover_ratio")
        width_column = self.attrs.get("width_column", "width")
        height_column = self.attrs.get("height_column", "height")
        ocr_cache_key_template = self.attrs.get("ocr_cache_key_template", "video_element_detection_{}_{}.json")
        valid_video_column = self.attrs.get("valid_video_column", "valid_video")
        duration_column = self.attrs.get("duration_column", "duration")

        kconf_params: dict = get_kconf_value("ad.algorithm.nieuwlandGeneration", "json")
        mask_subtitle_cfg: dict = kconf_params['mask_subtitle_cfg']
        subtitle_expand_bbox = mask_subtitle_cfg.get("subtitle_expand_bbox", [0, 0])
        subtitle_bg_expand_bbox = mask_subtitle_cfg.get("subtitle_bg_expand_bbox", [0, 0])
        expand_time = mask_subtitle_cfg.get("expand_time", [0.1, 0.1])
        center_range_w = mask_subtitle_cfg.get("center_range_w", [0.47, 0.53])
        center_range_h = mask_subtitle_cfg.get("center_range_h", [0.4, 0.6])
        height_outlier_th = mask_subtitle_cfg.get("height_outlier_th", 3)
        text_type = mask_subtitle_cfg.get("text_type", [1, 2, 4, 5, 8, 9])
        version = mask_subtitle_cfg.get("video_element_detection_version", "20231030")
        filter_ocr_area_th_rel = mask_subtitle_cfg.get("filter_ocr_area_th_rel")

        blob_client = BlobStoreClientManager().get_client("ad-nieuwland-material")
        video_ocr_client = ClientManager().get_client_by_name("VideoElementDetectClient")
        material_table[video_ocr_res_column] = None
        material_table[video_subtitle_list_column] = None
        material_table[video_ocr_cover_ratio_column] = None
        material_table[valid_video_column] = True
        for index, row in material_table.iterrows():
            video_blob_key = row.get(video_blob_key_column)
            db, table, rs_key = parse_bbs_resource_id(video_blob_key)
            rs_key_basename = os.path.splitext(rs_key)[0]
            ocr_cache_key = ocr_cache_key_template.format(rs_key_basename, version)
            ocr_info = None
            if blob_client.object_exists(ocr_cache_key):
                status, ocr_cache = blob_client.download_bytes_from_s3(ocr_cache_key)
                if status:
                    ocr_info = json.loads(ocr_cache)
            else:
                ocr_info = video_ocr_client.sync_req(video_blob_key)
                if ocr_info is not None:
                    ocr_info_json = json.dumps(ocr_info)
                    blob_client.upload_bytes_to_s3(ocr_info_json.encode(), ocr_cache_key)

            if ocr_info is None:
                logger.info(f"ocr_info is None, video_blob_key:{video_blob_key}")
                continue

            # 提取字幕
            video_duration = row.get(duration_column)
            video_size = [row.get(width_column), row.get(height_column)]
            if video_duration < 0.01 or video_size[0] == 0 or video_size[1] == 0:
                logger.error(f"video_duration[{video_duration}] or video_size[{video_size}] error,"
                             f"video_blob_key:{video_blob_key}")
                continue

            subtitle_list = extract_subtitle(ocr_info,
                                             resolution=video_size,
                                             duration=video_duration,
                                             subtitle_expand_bbox=subtitle_expand_bbox,
                                             subtitle_bg_expand_bbox=subtitle_bg_expand_bbox,
                                             expand_time=expand_time,
                                             center_range_w=center_range_w,
                                             center_range_h=center_range_h,
                                             height_outlier_th=height_outlier_th,
                                             text_type=text_type)

            # 计算字幕覆盖比例
            element_bbox_list = [item["bbox"] for item in subtitle_list]
            video_screen = np.zeros(video_size)
            for bbox in element_bbox_list:
                x1, y1, x2, y2 = int(bbox[0]), int(bbox[1]), int(bbox[2]), int(bbox[3])
                video_screen[y1:y2, x1:x2] = 1
            if video_size[0] > 0 and video_size[1] > 0:
                ocr_cover_ratio = (video_screen.sum()) / (video_size[0] * video_size[1])
                material_table.loc[index, video_ocr_cover_ratio_column] = ocr_cover_ratio
                if ocr_cover_ratio > filter_ocr_area_th_rel:
                    material_table.loc[index, valid_video_column] = False

            if ocr_info is not None:
                material_table.at[index, video_ocr_res_column] = ocr_info
            if subtitle_list is not None:
                material_table.at[index, video_subtitle_list_column] = subtitle_list

        op_context.output_tables.append(material_table)
        return True


op_register.register_op(VideoOcrDetectOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="video_blob_key_column", type="str", desc="视频blobstore地址列名") \
    .add_attr(name="video_ocr_res_column", type="str", desc="视频ocr检测结果列名") \
    .add_attr(name="video_ocr_cover_ratio_column", type="str", desc="视频ocr覆盖比例列名") \
    .add_attr(name="width_column", type="str", desc="视频宽度列名") \
    .add_attr(name="height_column", type="str", desc="视频高度列名") \
    .add_attr(name="ocr_cache_key_template", type="str", desc="ocr缓存key模板") \
    .add_attr(name="valid_video_column", type="str", desc="判断视频是否有效的列名") \
    .set_parallel(True)
