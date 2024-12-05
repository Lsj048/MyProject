import json
import os

from video_graph.common.client.client_manager import ClientManager
from video_graph.common.utils.blobstore import BlobStoreClientManager
from video_graph.common.utils.tools import parse_bbs_resource_id, video_split_with_start_end_time, find_files, \
    build_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoShotClipOp(Op):
    """
    【remote】视频切镜算子，对视频按镜头切分，输出切片信息

    Attributes:
        video_blob_key_column (str): 视频BlobKey所在的列名，默认为"video_blob_key"
        video_file_path_column (str): 视频文件路径列名，默认为"video_file_path"
        video_shot_clip_column (str): 视频切片列名，默认为"shot_clip"
        video_shot_clip_res_column (str): 视频切片rpc结果列名，默认为"shot_clip_resp"
        video_shot_clip_num_column (str): 视频切片数量列名，默认为"shot_clip_num"
        clip_cache_key_template (str): 切片缓存Key模板，默认为"{}_clip_info.json"
        time_shift_value (int): 切片时间偏移值，防止切不干净，默认为200ms
        shot_clip_duration_threshold (int): 切片时长阈值，默认为2000ms
        shot_clip_split (bool): 是否使用长片段切分，默认为False
        shot_clip_split_duration_threshold (int): 长片段切分时长阈值，默认为10000ms

    InputTables:
        material_table: 视频BlobKey所在的表格

    OutputTables:
        material_table: 添加了视频切片信息的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_process_op/video_shot_clip_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        video_blob_key_column = self.attrs.get("video_blob_key_column", "video_blob_key")
        video_file_path_column = self.attrs.get("video_file_path_column", "video_file_path")
        video_shot_clip_column = self.attrs.get("video_shot_clip_column", "shot_clip")
        video_shot_clip_res_column = self.attrs.get("video_shot_clip_res_column", "shot_clip_resp")
        video_shot_clip_num_column = self.attrs.get("video_shot_clip_num_column", "shot_clip_num")
        clip_cache_key_template = self.attrs.get("clip_cache_key_template", "{}_clip_info.json")
        time_shift_value = self.attrs.get("time_shift_value", 200)
        shot_clip_duration_threshold = self.attrs.get("shot_clip_duration_threshold", 1000)
        shot_clip_split = self.attrs.get("shot_clip_split", False)
        shot_clip_split_duration_threshold = self.attrs.get("shot_clip_split_duration_threshold", 20000)

        blob_client = BlobStoreClientManager().get_client("ad-nieuwland-material")
        video_shot_clip_client = ClientManager().get_client_by_name("VideoShotClipClient")
        material_table[video_shot_clip_column] = None
        material_table[video_shot_clip_res_column] = None
        material_table[video_shot_clip_num_column] = 0
        for index, row in material_table.iterrows():
            video_blob_key = row.get(video_blob_key_column)
            video_file_path = row.get(video_file_path_column)
            db, table, rs_key = parse_bbs_resource_id(video_blob_key)
            rs_key_basename = os.path.splitext(rs_key)[0]
            clip_info_key = clip_cache_key_template.format(rs_key_basename)
            clip_result = None
            if blob_client.object_exists(clip_info_key):
                status, clip_cache = blob_client.download_bytes_from_s3(clip_info_key)
                if status:
                    clip_result = json.loads(clip_cache)
            else:
                clip_info = video_shot_clip_client.sync_req(video_blob_key, save_clip=True)
                if clip_info is None:
                    continue

                clip_result = {"isSuccess": clip_info['success'],
                               "version": clip_info.get('version', 'transnetv2'),
                               "clips": []}
                for clip in clip_info['clips']:
                    video_clip = clip.get('video_clip', None)
                    if video_clip is None:
                        continue

                    db = video_clip.get('db', None)
                    table = video_clip.get('table', None)
                    key = video_clip.get('key', None)
                    if db is None or table is None or key is None:
                        continue

                    clip_data = {'start_time': clip.get('start_time', 0),
                                 'end_time': clip.get('end_time', 0),
                                 'resource_id': '_'.join([db, table, key])}
                    clip_result['clips'].append(clip_data)

                if clip_result["isSuccess"] and len(clip_result["clips"]) > 0:
                    clip_info_json = json.dumps(clip_result)
                    blob_client.upload_bytes_to_s3(clip_info_json.encode(), clip_info_key)

            shot_clip = []
            shot_clip_blob_client = BlobStoreClientManager().get_client("ad-smart-algorithm-storage")
            for clip in clip_result["clips"]:
                modify_start_time = int(clip["start_time"]) + time_shift_value
                modify_end_time = int(clip["end_time"]) - time_shift_value
                shot_clip_duration = modify_end_time - modify_start_time
                if shot_clip_duration >= shot_clip_duration_threshold:
                    # 是否对长片段二次切分
                    if shot_clip_split and shot_clip_duration > shot_clip_split_duration_threshold:
                        current_shot_clip = []
                        shot_clip_split_cache_key = os.path.splitext(clip["resource_id"])[0]
                        if blob_client.object_exists(shot_clip_split_cache_key):
                            status, cache_res = blob_client.download_bytes_from_s3(shot_clip_split_cache_key)
                            if status:
                                current_shot_clip = json.loads(cache_res)
                        else:
                            interval_range = shot_clip_split_duration_threshold / 2
                            status, shot_clip_file_pattern = video_split_with_start_end_time(
                                video_file_path, interval_range/1000.0, modify_start_time/1000.0, modify_end_time/1000.0)
                            # 切分失败时，使用完整切片
                            if not status:
                                shot_clip.append((modify_start_time, modify_end_time, clip["resource_id"]))
                                continue

                            dirname, file_pattern = os.path.split(shot_clip_file_pattern)
                            file_list = sorted(list(find_files(dirname, file_pattern)))
                            current_start_time = modify_start_time
                            for idx, file in enumerate(file_list):
                                shot_clip_key = os.path.split(file)[1]
                                shot_clip_blob_client.upload_file_with_retry(file, shot_clip_key)

                                resource_id = build_bbs_resource_id(['ad', 'smart-algorithm-storage', shot_clip_key])
                                current_end_time = min(current_start_time + interval_range, modify_end_time)
                                if current_end_time - current_start_time >= shot_clip_duration_threshold:
                                    current_shot_clip.append((current_start_time, current_end_time, resource_id))
                                current_start_time = current_end_time
                            # 写缓存
                            current_shot_clip_json = json.dumps(current_shot_clip)
                            blob_client.upload_bytes_to_s3(current_shot_clip_json, shot_clip_split_cache_key)
                        shot_clip.extend(current_shot_clip)
                    else:
                        shot_clip.append((modify_start_time, modify_end_time, clip["resource_id"]))
            material_table.at[index, video_shot_clip_column] = shot_clip
            material_table.at[index, video_shot_clip_res_column] = clip_result
            material_table.at[index, video_shot_clip_num_column] = len(shot_clip)

        op_context.output_tables.append(material_table)
        return True


op_register.register_op(VideoShotClipOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="video_blob_key_column", type="str", desc="视频blobstore地址列名") \
    .add_attr(name="video_file_path_column", type="str", desc="视频文件路径列名") \
    .add_attr(name="video_shot_clip_column", type="str", desc="视频切镜结果列名") \
    .add_attr(name="video_shot_clip_res_column", type="str", desc="视频切镜rpc结果列名") \
    .add_attr(name="video_shot_clip_num_column", type="str", desc="视频切镜数量列名") \
    .add_attr(name="clip_cache_key_template", type="str", desc="切片缓存key模板") \
    .add_attr(name="time_shift_value", type="int", desc="切片时间偏移值") \
    .add_attr(name="shot_clip_duration_threshold", type="int", desc="切片时长阈值") \
    .add_attr(name="shot_clip_split", type="bool", desc="是否使用长片段切分") \
    .add_attr(name="shot_clip_split_duration_threshold", type="int", desc="长片段切分时长阈值") \
    .set_parallel(True)
