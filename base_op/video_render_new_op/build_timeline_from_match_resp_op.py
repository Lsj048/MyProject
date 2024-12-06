import json

from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder, AssetInfo
from ks_editor_kernel.pb.CommonDraftBaseAssetModel_pb2 import TimeRangeModel

from video_graph.common.utils.tools import auto_size
from video_graph.common.utils.minecraft_tools import modify_asset_speed, set_asset_muted, modify_asset_volume, \
    get_asset_clip_range_start_time
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class BuildTimelineFromMatchRespOp(Op):
    """
    Function:
        根据匹配结果构建Timeline，根据匹配结果构建时间线，包括视频和音频

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        video_match_res_column (str): 视频匹配结果列名，默认为"video_match_res"。
        video_blob_key_column (str): 视频blob key列名，默认为"mask_ocr_blob_key"。
        tts_blob_key_column (str): tts blob key列名，默认为"tts_blob_key"。
        tts_duration_column (str): tts时长列名，默认为"tts_duration"。
        tts_volume (str): tts音量，默认为1.0。
        resolution_column (str): 分辨率列名，默认为"resolution"。
        tts_asset_id_column (str): tts asset id列名，默认为"tts_asset_id"。
        origin_video_blob_key_column (str): 原始视频blob key列名，默认为"video_blob_key"。
        video_from_column (str): 视频来源列名，默认为"video_from"。
        video_list_map_column (str): 视频列表映射列名，默认为"video_list_map"。
        video_clip_list_column (str): 视频剪辑列表列名，默认为"video_clip_list"。
        video_asset_id_column (str): 草稿中的视频资源id列名，默认为"video_asset_id"。
        binding_main_track (bool): 是否绑定到主视频轨道，默认为False。

    InputTables:
        shot_table: 镜头表格。
        material_table: 素材表格。
        timeline_table: 时间线表格。

    OutputTables:
        timeline_table: 构建后的时间线表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/build_timeline_from_match_resp_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        material_table: DataTable = op_context.input_tables[1]
        timeline_table: DataTable = op_context.input_tables[2]
        project_model_builder_column = self.attrs.get("project_model_builder_column", "project_model_builder")
        input_file_map_column = self.attrs.get("input_file_map_column", "input_file_map")
        video_match_res_column = self.attrs.get("video_match_res_column", "video_match_res")
        video_blob_key_column = self.attrs.get("video_blob_key_column", "mask_ocr_blob_key")
        tts_blob_key_column = self.attrs.get("tts_blob_key_column", "tts_blob_key")
        tts_duration_column = self.attrs.get("tts_duration_column", "tts_duration")
        tts_volume = self.attrs.get("tts_volume", 1.0)
        resolution_column = self.attrs.get("resolution_column", "resolution")
        tts_asset_id_column = self.attrs.get("tts_asset_id_column", "tts_asset_id")
        origin_video_blob_key_column = self.attrs.get("origin_video_blob_key_column", "video_blob_key")
        video_from_column = self.attrs.get("video_from_column", "video_from")
        video_list_map_column = self.attrs.get("video_list_map_column", "video_list_map")
        video_clip_list_column = self.attrs.get("video_clip_list_column", "video_clip_list")
        video_asset_id_column = self.attrs.get("video_asset_id_column", "video_asset_id")
        binding_main_track = self.attrs.get("binding_main_track", False)

        project_model_builder: EditSceneVideoProjectBuilder = timeline_table.at[0, project_model_builder_column]
        input_file_map: dict = timeline_table.at[0, input_file_map_column]
        resolution = timeline_table.at[0, resolution_column]

        video_list_map = {}
        video_clip_list = []
        video_asset_ids = []
        video_match_res = shot_table.at[0, video_match_res_column]
        for line_videos in video_match_res.get('clips_info'):
            tts_start_time = line_videos.get("tts_start_time")
            tts_end_time = line_videos.get("tts_end_time")
            tts_duration = tts_end_time - tts_start_time
            video_clips = line_videos.get("video_clips")
            video_clips_duration = line_videos.get("video_clips_duration")

            # 添加视频片段
            speed = video_clips_duration / tts_duration
            for video_clip in video_clips:
                video_idx = int(video_clip.get("video_idx"))
                video_start_time = video_clip.get("video_start_time")
                video_end_time = video_clip.get("video_end_time")
                video_duration = video_end_time - video_start_time

                target_material = material_table.loc[video_idx - 1]
                video_blob_key = target_material.get(video_blob_key_column)
                video_from = target_material.get(video_from_column, "private")
                video_size = (target_material.get("width"), target_material.get("height"))

                # 记录剪辑片段信息
                origin_video_blob_key = target_material.get(origin_video_blob_key_column)
                if video_from not in video_list_map:
                    video_list_map[video_from] = set()
                video_list_map[video_from].add(origin_video_blob_key)
                video_clip_list.append((video_blob_key, video_start_time, video_end_time, video_from))

                if resolution[0] < resolution[1]:
                    target_size = auto_size(video_size, resolution, mode="align_width")
                else:
                    target_size = auto_size(video_size, resolution, mode="align_height")
                video_asset_id = project_model_builder.add_main_track_asset(
                    AssetInfo(path=video_blob_key,
                              duration=video_duration,
                              width=target_size[0],
                              height=target_size[1],
                              clip_range=TimeRangeModel(startTime=video_start_time, endTime=video_end_time))
                )
                video_asset_ids.append(str(video_asset_id))
                input_file_map.update({video_blob_key: video_blob_key})
                project_model_builder.modify_asset_model(video_asset_id, writer=modify_asset_speed(speed))
                project_model_builder.modify_asset_model(video_asset_id, writer=set_asset_muted)

        binding_video_asset_id = int(video_asset_ids[0]) if len(video_asset_ids) > 0 else 0
        # 如果需要绑定，则DisplayRange的startTime为宿主的ClipRange的startTime
        if binding_video_asset_id != 0 and binding_main_track:
            binding_video_start_time = get_asset_clip_range_start_time(project_model_builder, binding_video_asset_id)
        else:
            binding_video_start_time = 0

        tts_blob_key = shot_table.at[0, tts_blob_key_column]
        tts_duration = shot_table.at[0, tts_duration_column]
        tts_asset_id = project_model_builder.add_audio_asset(
            AssetInfo(path=tts_blob_key,
                      duration=tts_duration,
                      clip_range=TimeRangeModel(startTime=0, endTime=tts_duration),
                      real_range=TimeRangeModel(startTime=binding_video_start_time,
                                                endTime=binding_video_start_time + tts_duration))
        )
        input_file_map.update({tts_blob_key: tts_blob_key})
        project_model_builder.modify_asset_model(tts_asset_id, modify_asset_volume(tts_volume))
        timeline_table.at[0, tts_asset_id_column] = str(tts_asset_id)

        # 音频绑定到主轨
        if binding_video_asset_id != 0 and binding_main_track:
            project_model_builder.set_bind_track_id_of_asset(tts_asset_id, binding_video_asset_id)

        project_model_builder.align_main_track()
        timeline_table[video_list_map_column] = None
        timeline_table[video_clip_list_column] = None
        timeline_table.at[0, video_list_map_column] = video_list_map
        timeline_table.at[0, video_clip_list_column] = json.dumps(video_clip_list)

        if video_asset_id_column in timeline_table.columns:
            video_asset_id_list = timeline_table.at[0, video_asset_id_column]
            video_asset_id_list.extend(video_asset_ids)
        else:
            timeline_table[video_asset_id_column] = None
            timeline_table.at[0, video_asset_id_column] = video_asset_ids

        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(BuildTimelineFromMatchRespOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="material_table", type="DataTable", desc="物料表") \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="project_model_builder_column", type="str", desc="项目模型构建列名") \
    .add_attr(name="input_file_map_column", type="str", desc="输入文件映射列名") \
    .add_attr(name="video_match_res_column", type="str", desc="视频匹配结果列名") \
    .add_attr(name="video_blob_key_column", type="str", desc="视频blob地址列名") \
    .add_attr(name="tts_blob_key_column", type="str", desc="音频blob地址列名") \
    .add_attr(name="tts_duration_column", type="str", desc="音频时长列名") \
    .add_attr(name="resolution_column", type="str", desc="分辨率列名") \
    .add_attr(name="tts_asset_id_column", type="str", desc="tts资源id列名")
