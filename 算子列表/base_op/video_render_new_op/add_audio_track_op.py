import math

from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder, AssetInfo
from ks_editor_kernel.pb.CommonDraftBaseAssetModel_pb2 import TimeRangeModel

from video_graph.common.utils.minecraft_tools import modify_asset_volume, get_asset_clip_range_start_time
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddAudioTrackOp(Op):
    """
    Function:
        增加音频轨算子，video_index为-1时，表示批量增加音频到轨道上，可以选择stack和append两种模式，否则表示增加单个音频到轨道上
        * stack：叠放模式，音频时间可重叠
        * append：追加模式，音频时间不可重叠

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        audio_blob_key_column (str): 音频blob key列名，默认为"audio_blob_key"。
        audio_duration_column (str): 音频时长列名，默认为"audio_duration"。
        audio_type (str): 音频类型，默认为"bgm"。
        loop_play (bool): 是否循环播放，默认为True。
        audio_volume (float): 音频音量，默认为1.0。
        audio_duration_tail (int): 音频时长尾巴，默认为0。
        audio_index (int): 音频索引，默认为0。
        start_time_column (str): 开始时间列名，默认为"audio_start_time"。
        end_time_column (str): 结束时间列名，默认为"audio_end_time"。
        placement_mode (str): 放置模式，默认为"stack"，可选值为"stack"（音频时间可重叠）和"append"（音频时间不可重叠）。
        video_asset_id_column (str): 草稿中的视频资源id列名，默认为"video_asset_id"。
        binding_main_track (bool): 是否绑定到主视频轨道，默认为False。

    InputTables:
        timeline_table: 时间线表格。
        shot_table: 镜头表格。

    OutputTables:
        timeline_table: 添加了音频轨的时间线表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/add_audio_track_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """
    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        shot_table: DataTable = op_context.input_tables[1]
        project_model_builder_column = self.attrs.get("project_model_builder_column", "project_model_builder")
        input_file_map_column = self.attrs.get("input_file_map_column", "input_file_map")
        audio_blob_key_column = self.attrs.get("audio_blob_key_column", "audio_blob_key")
        audio_duration_column = self.attrs.get("audio_duration_column", "audio_duration")
        audio_type = self.attrs.get("audio_type", "bgm")
        loop_play = self.attrs.get("loop_play", True)
        audio_volume = self.attrs.get("audio_volume", 1.0)
        audio_duration_tail = self.attrs.get("audio_duration_tail", 0)
        audio_index = self.attrs.get("audio_index", 0)
        start_time_column = self.attrs.get("start_time_column", "audio_start_time")
        end_time_column = self.attrs.get("end_time_column", "audio_end_time")
        placement_mode = self.attrs.get("placement_mode", "stack")
        video_asset_id_column = self.attrs.get("video_asset_id_column", "video_asset_id")
        binding_main_track = self.attrs.get("binding_main_track", False)

        project_model_builder: EditSceneVideoProjectBuilder = timeline_table.at[0, project_model_builder_column]
        input_file_map: dict = timeline_table.at[0, input_file_map_column]
        video_asset_ids = timeline_table.at[0, video_asset_id_column]
        binding_video_asset_id = int(video_asset_ids[0]) if len(video_asset_ids) > 0 else 0
        # 如果需要绑定，则DisplayRange的startTime为宿主的ClipRange的startTime
        if binding_video_asset_id != 0 and binding_main_track:
            binding_video_start_time = get_asset_clip_range_start_time(project_model_builder, binding_video_asset_id)
        else:
            binding_video_start_time = 0
        project_model_builder.bind_time_model_guru_video_project()
        main_track_duration = project_model_builder.get_project_duration()

        if 0 <= audio_index < shot_table.shape[0]:
            audio_key = shot_table.loc[0, audio_blob_key_column]
            audio_duration = shot_table.loc[0, audio_duration_column] + audio_duration_tail

            loop_num = 1
            if loop_play and main_track_duration > audio_duration:
                loop_num = math.ceil(main_track_duration / audio_duration)

            audio_asset_id = 0
            current_duration = 0
            current_real_duration = binding_video_start_time
            for idx in range(loop_num):
                remain_duration = min(audio_duration, main_track_duration - current_duration)
                audio_asset_id = project_model_builder.add_audio_asset(
                    AssetInfo(path=audio_key,
                              duration=audio_duration,
                              clip_range=TimeRangeModel(startTime=0, endTime=remain_duration),
                              real_range=TimeRangeModel(startTime=current_real_duration, endTime=current_real_duration+remain_duration))
                )
                current_duration += audio_duration
                current_real_duration += audio_duration
                input_file_map.update({audio_key: audio_key})
                project_model_builder.modify_asset_model(audio_asset_id, modify_asset_volume(audio_volume))
                if binding_video_asset_id != 0 and binding_main_track:
                    project_model_builder.set_bind_track_id_of_asset(audio_asset_id, binding_video_asset_id)

            timeline_table.at[0, f"{audio_type}_asset_id"] = str(audio_asset_id)
            timeline_table.at[0, f"{audio_type}_blob_key"] = audio_key
        elif audio_index == -1:
            current_duration = binding_video_start_time
            for index, row in shot_table.iterrows():
                audio_key = row.get(audio_blob_key_column)

                if placement_mode == "stack":  # 叠放模式
                    start_time = row.get(start_time_column)
                    end_time = row.get(end_time_column)
                    audio_duration = end_time - start_time + audio_duration_tail
                    audio_asset_id = project_model_builder.add_audio_asset(
                        AssetInfo(path=audio_key,
                                  duration=audio_duration,
                                  clip_range=TimeRangeModel(startTime=0, endTime=audio_duration),
                                  real_range=TimeRangeModel(startTime=current_duration+start_time, endTime=current_duration+end_time))
                    )
                    input_file_map.update({audio_key: audio_key})
                    project_model_builder.modify_asset_model(audio_asset_id, modify_asset_volume(audio_volume))
                    if binding_video_asset_id != 0 and binding_main_track:
                        project_model_builder.set_bind_track_id_of_asset(audio_asset_id, binding_video_asset_id)
                elif placement_mode == "append":  # 追加模式
                    audio_duration = row.get(audio_duration_column) + audio_duration_tail
                    audio_asset_id = project_model_builder.add_audio_asset(
                        AssetInfo(path=audio_key,
                                  duration=audio_duration,
                                  clip_range=TimeRangeModel(startTime=0, endTime=audio_duration),
                                  real_range=TimeRangeModel(startTime=current_duration, endTime=current_duration+audio_duration))
                    )
                    current_duration += audio_duration
                    input_file_map.update({audio_key: audio_key})
                    project_model_builder.modify_asset_model(audio_asset_id, modify_asset_volume(audio_volume))
                    if binding_video_asset_id != 0 and binding_main_track:
                        project_model_builder.set_bind_track_id_of_asset(audio_asset_id, binding_video_asset_id)

        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddAudioTrackOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="project_model_builder_column", type="str", desc="项目模型构建列名") \
    .add_attr(name="input_file_map_column", type="str", desc="输入文件映射列名") \
    .add_attr(name="audio_blob_key_column", type="str", desc="音频blob key列名") \
    .add_attr(name="audio_duration_column", type="str", desc="音频时长列名") \
    .add_attr(name="audio_type", type="str", desc="音频类型") \
    .add_attr(name="loop_play", type="bool", desc="是否循环播放") \
    .add_attr(name="audio_volume", type="float", desc="音频音量") \
    .add_attr(name="audio_duration_tail", type="float", desc="音频时长尾巴")
