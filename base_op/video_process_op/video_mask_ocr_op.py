from video_graph.common.client.client_manager import ClientManager
from video_graph.common.utils.blobstore import BlobStoreClientManager
from video_graph.common.utils.kconf import get_kconf_value
from video_graph.common.utils.logger import logger
from video_graph.common.utils.tools import make_mask_render_req, parse_bbs_resource_id, \
    build_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoMaskOcrOp(Op):
    """
    【remote】视频字幕擦除算子，输出视频字幕擦除后视频的blob_key

    Attributes:
        video_blob_key_column (str): 视频BlobKey所在的列名，默认为"video_blob_key"
        video_mask_ocr_res_column (str): 视频字幕擦除结果列名，默认为"mask_ocr_blob_key"
        server_version (str): 服务版本，默认为"gaussian"
        subtitle_list_column (str): 字幕列表列名，默认为"subtitle_list"
        blob_db (str): Blob数据库名称，默认为"ad"
        blob_table (str): Blob表名称，默认为"nieuwland-material"
        duration_column (str): 视频时长列名，默认为"duration"
        width_column (str): 视频宽度列名，默认为"width"
        height_column (str): 视频高度列名，默认为"height"
        biz_name (str): 提交高斯模糊任务的业务名, 默认为"nieuwland"
        inpainting_prefix (str): inpainting结果的前缀，用于区分不同版本的inpainting结果

    InputTables:
        material_table: 视频BlobKey所在的表格

    OutputTables:
        material_table: 添加了视频字幕擦除结果的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_process_op/video_mask_ocr_op.py?ref_type=heads

    Examples:
        #等待作者更新～
    """

    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        video_blob_key_column = self.attrs.get("video_blob_key_column", "video_blob_key")
        video_mask_ocr_res_column = self.attrs.get("video_mask_ocr_res_column", "mask_ocr_blob_key")
        server_version = self.attrs.get("server_version", "gaussian")
        subtitle_list_column = self.attrs.get("subtitle_list_column", "subtitle_list")
        blob_db = self.attrs.get("blob_db", "ad")
        blob_table = self.attrs.get("blob_table", "nieuwland-material")
        duration_column = self.attrs.get("duration_column", "duration")
        width_column = self.attrs.get("width_column", "width")
        height_column = self.attrs.get("height_column", "height")
        biz_name = self.attrs.get("biz_name", "nieuwland")
        inpainting_prefix = self.attrs.get("inpainting_prefix")

        kconf_params: dict = get_kconf_value("ad.algorithm.nieuwlandGeneration", "json")
        mask_subtitle_cfg: dict = kconf_params['mask_subtitle_cfg']
        inpainting_version = mask_subtitle_cfg.get("inpainting_version")
        version = mask_subtitle_cfg.get("inpainting_version" if server_version == "inpainting"
                                        else "gaussion_blur_version")

        blob_client = BlobStoreClientManager().get_client(f"{blob_db}-{blob_table}")
        client_name = "VideoInpaintingYTechClient" if server_version == "inpainting" else "VideoRenderClient"
        video_mask_ocr_client = ClientManager().get_client_by_name(client_name)
        if not video_mask_ocr_client:
            return False

        material_table[video_mask_ocr_res_column] = None
        for index, row in material_table.iterrows():
            video_blob_key = row.get(video_blob_key_column)
            subtitle_list = row.get(subtitle_list_column)
            db, table, key = parse_bbs_resource_id(video_blob_key)
            output_key = build_bbs_resource_id([blob_db, blob_table, f"mask_subtitle_{version}_{key}"])

            # 无字幕区域，无需遮盖
            if not subtitle_list:
                material_table.loc[index, video_mask_ocr_res_column] = video_blob_key
                continue

            # 检查缓存，优先用inpainting的缓存结果
            inpainting_key = f"{inpainting_prefix}_{key}" if inpainting_prefix else f"mask_subtitle_{inpainting_version}_{key}"
            if blob_client.object_exists(inpainting_key):
                inpainting_output_key = build_bbs_resource_id([blob_db, blob_table, inpainting_key])
                material_table.loc[index, video_mask_ocr_res_column] = inpainting_output_key
                continue
            elif inpainting_version != version and blob_client.object_exists(f"mask_subtitle_{version}_{key}"):
                material_table.loc[index, video_mask_ocr_res_column] = output_key
                continue

            video_duration = row.get(duration_column)
            video_size = [row.get(width_column), row.get(height_column)]
            mask_ocr_status = False
            if server_version == "inpainting":
                bboxes = []
                for item in subtitle_list:
                    bbox = item["bbox"]
                    start_time = item["start_time"]
                    end_time = item["end_time"]
                    bboxes.append(
                        f"{start_time},{end_time},{bbox[0]},{bbox[1]},{bbox[2] - bbox[0]},{bbox[3] - bbox[1]}")
                resp = video_mask_ocr_client.sync_req(video_blob_key, bboxes, output_key)
                if resp and resp['status'] == 'SUCCESS':
                    mask_ocr_status = True
            elif server_version == "gaussian":
                filter_ocr_height_th_rel = mask_subtitle_cfg["filter_ocr_height_th_rel"]
                req = make_mask_render_req(video_blob_key, output_key, video_duration, video_size,
                                           subtitle_list, mask_subtitle_cfg,
                                           filter_ocr_height_th=video_size[1] * filter_ocr_height_th_rel,
                                           mask_overlap_pos_th=mask_subtitle_cfg["mask_overlap_pos_th"],
                                           mask_overlap_time_th=mask_subtitle_cfg["mask_overlap_time_th"])
                if req:
                    req.biz = biz_name
                    resp = video_mask_ocr_client.sync_req(req)
                    if resp and resp['status'] == 'SUCCESS':
                        mask_ocr_status = True
                else:
                    logger.info(f"video_blob_key[{video_blob_key}] gaussian mask ocr req is None")

            if mask_ocr_status:
                material_table.loc[index, video_mask_ocr_res_column] = output_key

        op_context.output_tables.append(material_table)
        return True


op_register.register_op(VideoMaskOcrOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="video_blob_key_column", type="str", desc="视频blobstore地址列名") \
    .add_attr(name="video_mask_ocr_res_column", type="str", desc="视频字幕擦除结果列名") \
    .add_attr(name="server_version", type="str", desc="擦除服务版本，inpainting or gaussian") \
    .add_attr(name="subtitle_list_column", type="str", desc="字幕列表列名") \
    .add_attr(name="blob_db", type="str", desc="blob db") \
    .add_attr(name="blob_table", type="str", desc="blob table") \
    .add_attr(name="duration_column", type="str", desc="视频时长列名") \
    .add_attr(name="width_column", type="str", desc="视频宽度列名") \
    .add_attr(name="height_column", type="str", desc="视频高度列名") \
    .set_parallel(True)
