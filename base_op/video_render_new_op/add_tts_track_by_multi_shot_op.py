from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder, AssetInfo
from ks_editor_kernel.pb.CommonDraftBaseAssetModel_pb2 import TimeRangeModel

from video_graph.common.utils.minecraft_tools import modify_asset_volume
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddTtsTrackByMultiShotOp(Op):
    """
    【local】从标准输入表中添加TTS，基于audio_blob_key、audio_duration、need_tts三个字段来生成

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        audio_blob_key_column (str): 音频blob key列名，默认为"audio_blob_key"。
        audio_duration_column (str): 音频时长列名，默认为"audio_duration"。
        audio_volume (float): 音频音量，默认为1.0。
        audio_duration_tail (int): 音频时长尾部，默认为0。
        need_tts_column (str): 是否需要TTS列名，默认为"need_tts"。
        tts_asset_ids_column (str): TTS asset id列名，默认为"tts_asset_ids"。

    InputTables:
        timeline_table: 时间线表格。
        shot_table: 镜头表格。

    OutputTables:
        timeline_table: 添加了TTS轨的时间线表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/add_tts_track_by_multi_shot_op.py?ref_type=heads

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
        audio_volume = self.attrs.get("audio_volume", 1.0)
        audio_duration_tail = self.attrs.get("audio_duration_tail", 0)
        need_tts_column = self.attrs.get("need_tts_column", "need_tts")
        tts_asset_ids_column = self.attrs.get("tts_asset_ids_column", "tts_asset_ids")

        project_model_builder: EditSceneVideoProjectBuilder = timeline_table.at[0, project_model_builder_column]
        input_file_map: dict = timeline_table.at[0, input_file_map_column]

        current_duration = 0
        audio_asset_ids = []
        for index, row in shot_table.iterrows():
            audio_key = row.get(audio_blob_key_column)
            audio_duration = row.get(audio_duration_column) + audio_duration_tail
            need_tts = row.get(need_tts_column)
            if not need_tts:
                current_duration += audio_duration
                continue

            audio_asset_id = project_model_builder.add_audio_asset(
                AssetInfo(path=audio_key,
                          duration=audio_duration,
                          clip_range=TimeRangeModel(startTime=0, endTime=audio_duration),
                          real_range=TimeRangeModel(startTime=current_duration,
                                                    endTime=current_duration + audio_duration))
            )
            audio_asset_ids.append(audio_asset_id)
            current_duration += audio_duration
            input_file_map.update({audio_key: audio_key})
            project_model_builder.modify_asset_model(audio_asset_id, modify_asset_volume(audio_volume))

        timeline_table[tts_asset_ids_column] = None
        timeline_table.at[0, tts_asset_ids_column] = audio_asset_ids
        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddTtsTrackByMultiShotOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表")
