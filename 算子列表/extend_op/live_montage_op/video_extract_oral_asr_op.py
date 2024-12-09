import json
import os

from video_graph import logger
from video_graph.common.utils.blobstore import BlobStoreClientManager
from video_graph.common.utils.parse_photo import ParsePhotoV2
from video_graph.common.utils.tools import parse_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoExtractOralASROp(Op):
    """
    Function:
        提取视频口播片段ASR信息，从blobstore中获取

    Attributes:
        file_blob_key_column (str): 输入表格中存储文件 Blob Key 的列名。
        oral_asr_column (str, optional): 视频文件路径所在的列名，默认为"oral_asr"。
        asr_texts_column (str, optional): ASR文本所在的列名，默认为"asr_texts"。
        asr_start_end_column (str, optional): ASR文本起止时间所在的列名，默认为"asr_start_end"。
        parse_res_template (str, optional): 已挖掘素材混存视频解析Blobstore存储地址模版，默认"ad_nieuwland-material_{}_parse_photo_v2.json"。
        online (bool, optional): 实时在线抽取，默认为False
        set_cache (bool, optional): 实时在线抽取, 缓存中间变量，默认为False

    InputTables:
        material_table: 视频文件所在的表格

    OutputTables:
        material_table: 添加了口播信息的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/extend_op/live_montage_op/video_extract_oral_asr_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        file_blob_key_column = self.attrs.get("file_blob_key_column", "file_blob_key")
        oral_asr_column = self.attrs.get("oral_asr_column", "oral_asr")
        asr_texts_column = self.attrs.get("asr_texts_column", "asr_texts")
        asr_start_end_column = self.attrs.get("asr_start_end_column", "asr_start_end")
        parse_res_template = self.attrs.get("parse_res_template", "ad_nieuwland-material_{}_parse_photo_v2.json")
        asr_res_template = self.attrs.get("asr_res_template", "ad_nieuwland-material_{}_photo_asr.json")
        read_parse_cache = self.attrs.get("read_parse_cache", False)
        online = self.attrs.get("online", False)
        set_cache = self.attrs.get("set_cache", False)

        status = False
        material_table[oral_asr_column] = None
        material_table[asr_texts_column] = None
        material_table[asr_start_end_column] = None
        for index, row in material_table.iterrows():
            file_blob_key = row.get(file_blob_key_column)
            if str(file_blob_key) == 'nan':
                continue

            if online:
                parser = ParsePhotoV2(parse_res_template=parse_res_template,
                                      asr_res_template=asr_res_template)
                script_clips = parser.parse(file_blob_key, set_cache=set_cache, read_cache=read_parse_cache)

                if script_clips is None:
                    logger.error(f"Parsing {file_blob_key} failed")
                    continue
            else:
                db, table, key = parse_bbs_resource_id(file_blob_key)
                parse_res_blob_key = parse_res_template.format(os.path.splitext(key)[0])
                _db, _table, _key = parse_bbs_resource_id(parse_res_blob_key)
                blob_client = BlobStoreClientManager().get_client(f"{_db}-{_table}")
                status, script_cache = blob_client.download_bytes_from_s3(_key)
                if not status:
                    continue
                script_clips = json.loads(script_cache)

            oral_asr = []
            for script_clip in script_clips:
                if script_clip.get("has_speaker", False):
                    oral_asr.extend(script_clip.get('caption_path', []))

            material_table.at[index, oral_asr_column] = oral_asr
            material_table.at[index, asr_texts_column] = [t for (t, s, e) in oral_asr]
            material_table.at[index, asr_start_end_column] = [[s, e] for (t, s, e) in oral_asr]
            status = True

        op_context.output_tables.append(material_table)
        return status


op_register.register_op(VideoExtractOralASROp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="file_blob_key_column", type="str", desc="输入表格中存储文件 Blob Key 的列名") \
    .add_attr(name="oral_asr_column", type="str", desc="视频文件路径所在的列名，默认为oral_asr") \
    .add_attr(name="parse_res_template", type="str",
              desc="已挖掘素材缓存视频解析Blobstore存储地址模版，默认ad_nieuwland-material_{}_parse_photo_v2.json") \
    .add_attr(name="asr_res_template", type="str",
              desc="缓存ASR的Blobstore存储地址模版，默认ad_nieuwland-material_{}_photo_asr.json") \
    .add_attr(name="online", type="bool", desc="实时在线抽取，默认为False") \
    .add_attr(name="set_cache", type="bool", desc="实时在线抽取，存储缓存中间变量，默认为False") \
    .add_attr(name="read_parse_cache", type="bool", desc="实时在线抽取，读取缓存变量，默认为False") \
    .set_parallel(True)
