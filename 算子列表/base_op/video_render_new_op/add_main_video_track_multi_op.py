from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder, AssetInfo
from ks_editor_kernel.pb.CommonDraftBaseAssetModel_pb2 import TimeRangeModel

from video_graph.common.utils.minecraft_tools import set_asset_muted
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddMainVideoTrackMultiOp(Op):
    """
    Function:
        批量增加主视频轨算子，应对有多个时间轨道的情况，将material_table的每一行作为一个视频添加到timeline_table对应行的时间轴上

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        video_blob_key_column (str): 视频blob key列名，默认为"video_blob_key"。
        duration_column (str): 时长列名，默认为"duration"。
        is_muted (bool): 是否静音，默认为True。

    InputTables:
        timeline_table: 时间线表格。
        material_table: 素材表格。

    OutputTables:
        timeline_table: 添加了视频轨的时间线表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/add_main_video_track_multi_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """
    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        material_table: DataTable = op_context.input_tables[1]
        project_model_builder_column = self.attrs.get("project_model_builder_column", "project_model_builder")
        input_file_map_column = self.attrs.get("input_file_map_column", "input_file_map")
        video_blob_key_column = self.attrs.get("video_blob_key_column", "video_blob_key")
        duration_column = self.attrs.get("duration_column", "duration")
        is_muted = self.attrs.get("is_muted", True)
        video_asset_id_column = self.attrs.get("video_asset_id_column", "video_asset_id")

        for index, row in timeline_table.iterrows():
            input_file_map: dict = row.get(input_file_map_column)
            project_model_builder: EditSceneVideoProjectBuilder = row.get(project_model_builder_column)

            if material_table.shape[0] == index:
                break

            video_blob_key = material_table.loc[index, video_blob_key_column]
            duration = material_table.loc[index, duration_column]

            input_file_map.update({video_blob_key: video_blob_key})
            video_asset_id = project_model_builder.add_main_track_asset(AssetInfo(
                path=video_blob_key, duration=duration, clip_range=TimeRangeModel(startTime=0, endTime=duration)
            ))
            project_model_builder.align_main_track()
            if is_muted:
                project_model_builder.modify_asset_model(video_asset_id, writer=set_asset_muted)

            if video_asset_id_column in timeline_table.columns:
                video_asset_id_list = timeline_table.at[index, video_asset_id_column]
                video_asset_id_list.append(str(video_asset_id))
            else:
                timeline_table[video_asset_id_column] = [[] for _ in range(timeline_table.shape[0])]
                timeline_table.at[index, video_asset_id_column] = [str(video_asset_id)]

        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddMainVideoTrackMultiOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_input(name="material_table", type="DataTable", desc="物料表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="project_model_builder_column", type="str", desc="项目模型构建列名") \
    .add_attr(name="input_file_map_column", type="str", desc="输入文件映射列名") \
    .add_attr(name="video_blob_key_column", type="str", desc="视频blob key列名") \
    .add_attr(name="duration_column", type="str", desc="时长列名") \
    .add_attr(name="is_muted", type="bool", desc="是否静音")
