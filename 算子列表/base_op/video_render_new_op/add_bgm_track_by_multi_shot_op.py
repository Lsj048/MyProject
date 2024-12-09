import math

from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder, AssetInfo
from ks_editor_kernel.pb.CommonDraftBaseAssetModel_pb2 import TimeRangeModel

from video_graph.common.utils.minecraft_tools import modify_asset_volume
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddBgmTrackByMultiShotOp(Op):
    """
    Function:
        从标准输入表中添加BGM，基于audio_blob_key、audio_duration、need_bgm、shot_duration四个字段来生成，如果bgm时长短于镜号时长，会自动循环播放

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        audio_blob_key_column (str): 音频blob key列名，默认为"audio_blob_key"。
        audio_duration_column (str): 音频时长列名，默认为"audio_duration"。
        audio_volume (float): 音频音量，默认为0.3。
        need_bgm_column (str): 是否需要BGM列名，默认为"need_bgm"。
        bgm_asset_ids_column (str): BGM asset id列名，默认为"bgm_asset_ids"。
        shot_duration_column (str): 镜头时长列名，默认为"shot_duration"。

    InputTables:
        timeline_table: 时间线表格。
        shot_table: 镜头表格。

    OutputTables:
        timeline_table: 添加了BGM轨的时间线表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/add_bgm_track_by_multi_shot_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        shot_table: DataTable = op_context.input_tables[1]
        project_model_builder_column = self.attrs.get("project_model_builder_column", "project_model_builder")
        input_file_map_column = self.attrs.get("input_file_map_column", "input_file_map")
        audio_blob_key_column = self.attrs.get("audio_blob_key_column", "audio_blob_key")
        audio_duration_column = self.attrs.get("audio_duration_column", "audio_duration")
        audio_volume = self.attrs.get("audio_volume", 0.3)
        need_bgm_column = self.attrs.get("need_bgm_column", "need_bgm")
        bgm_asset_ids_column = self.attrs.get("bgm_asset_ids_column", "bgm_asset_ids")
        shot_duration_column = self.attrs.get("shot_duration_column", "shot_duration")

        project_model_builder: EditSceneVideoProjectBuilder = timeline_table.at[0, project_model_builder_column]
        input_file_map: dict = timeline_table.at[0, input_file_map_column]

        bgm_clips = []
        bgm_blob_key = ""
        bgm_duration = 0
        current_start_time = 0
        current_end_time = 0
        for index, row in shot_table.iterrows():
            shot_duration = row.get(shot_duration_column)
            need_bgm = row.get(need_bgm_column)
            if need_bgm:
                current_end_time += shot_duration
                bgm_blob_key = row.get(audio_blob_key_column)
                bgm_duration = row.get(audio_duration_column)
            else:
                if current_end_time > current_start_time:
                    bgm_clips.append((current_start_time, current_end_time))
                current_end_time += shot_duration
                current_start_time = current_end_time
        if current_end_time > current_start_time:
            bgm_clips.append((current_start_time, current_end_time))

        audio_asset_ids = []
        for start_time, end_time in bgm_clips:
            clip_duration = end_time - start_time
            loop_num = math.ceil(clip_duration / bgm_duration) if clip_duration > bgm_duration else 1

            current_duration = start_time
            for idx in range(loop_num):
                loop_end_time = current_duration + bgm_duration \
                    if current_duration + bgm_duration < end_time else end_time
                audio_asset_id = project_model_builder.add_audio_asset(
                    AssetInfo(path=bgm_blob_key,
                              duration=loop_end_time - current_duration,
                              clip_range=TimeRangeModel(startTime=0, endTime=loop_end_time - current_duration),
                              real_range=TimeRangeModel(startTime=current_duration,
                                                        endTime=loop_end_time))
                )
                audio_asset_ids.append(audio_asset_id)
                current_duration += bgm_duration
                input_file_map.update({bgm_blob_key: bgm_blob_key})
                project_model_builder.modify_asset_model(audio_asset_id, modify_asset_volume(audio_volume))

        timeline_table[bgm_asset_ids_column] = None
        timeline_table.at[0, bgm_asset_ids_column] = audio_asset_ids
        timeline_table.at[0, "bgm_blob_key"] = bgm_blob_key
        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddBgmTrackByMultiShotOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表")
