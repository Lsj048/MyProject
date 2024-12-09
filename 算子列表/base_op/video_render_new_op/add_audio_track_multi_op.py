import math

from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder, AssetInfo
from ks_editor_kernel.pb.CommonDraftBaseAssetModel_pb2 import TimeRangeModel

from video_graph.common.utils.minecraft_tools import modify_asset_volume
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddAudioTrackMultiOp(Op):
    """
    Function:
        批量增加音频轨算子，应对有多个时间轨道的情况，将shot_table的每一行作为一个音频添加到timeline_table对应行的时间轴上

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        audio_blob_key_column (str): 音频blob key列名，默认为"audio_blob_key"。
        audio_duration_column (str): 音频时长列名，默认为"audio_duration"。
        audio_type (str): 音频类型，默认为"bgm"。
        loop_play (bool): 是否循环播放，默认为True。
        audio_volume (float): 音频音量，默认为1.0。

    InputTables:
        timeline_table: 时间线表格。
        shot_table: 镜头表格。

    OutputTables:
        timeline_table: 添加了音频轨的时间线表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/add_audio_track_multi_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute_when_skip(self, op_context: OpContext) -> bool:
        op_context.output_tables.append(op_context.input_tables[0])
        return True

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

        for index, row in timeline_table.iterrows():
            project_model_builder: EditSceneVideoProjectBuilder = timeline_table.at[index, project_model_builder_column]
            input_file_map: dict = timeline_table.at[index, input_file_map_column]

            if shot_table.shape[0] == index:
                break

            audio_key = shot_table.loc[index, audio_blob_key_column]
            audio_duration = shot_table.loc[index, audio_duration_column]
            if not audio_key or not audio_duration:
                continue

            project_model_builder.bind_time_model_guru_video_project()
            main_track_duration = project_model_builder.get_project_duration()

            loop_num = 1
            if loop_play and main_track_duration > audio_duration:
                loop_num = math.ceil(main_track_duration / audio_duration)

            audio_asset_id = 0
            current_duration = 0
            for idx in range(loop_num):
                remain_duration = min(audio_duration, main_track_duration - current_duration)
                audio_asset_id = project_model_builder.add_audio_asset(
                    AssetInfo(path=audio_key,
                              duration=audio_duration,
                              clip_range=TimeRangeModel(startTime=0, endTime=remain_duration),
                              real_range=TimeRangeModel(startTime=current_duration,
                                                        endTime=current_duration + remain_duration))
                )
                current_duration += audio_duration
                input_file_map.update({audio_key: audio_key})
                project_model_builder.modify_asset_model(audio_asset_id, modify_asset_volume(audio_volume))

            timeline_table.at[0, f"{audio_type}_asset_id"] = str(audio_asset_id)
        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddAudioTrackMultiOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="project_model_builder_column", type="str", desc="项目模型构建列名") \
    .add_attr(name="input_file_map_column", type="str", desc="输入文件映射列名") \
    .add_attr(name="audio_blob_key_column", type="str", desc="音频blob key列名")
