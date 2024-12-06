from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class CreateMinecraftRequestNewOp(Op):
    """
    Function:
        构造minecraft request新算子，可以构造一个或多个request

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        resolution_column (str): 分辨率列名，默认为"resolution"。
        row_num (int): 行数，默认为1。

    InputTables:
        timeline_table: 时间线表格。

    OutputTables:
        timeline_table: 添加了项目模型构建器、输入文件映射和分辨率的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/create_minecraft_request_new_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        project_model_builder_column = self.attrs.get("project_model_builder_column", "project_model_builder")
        input_file_map_column = self.attrs.get("input_file_map_column", "input_file_map")
        resolution_column = self.attrs.get("resolution_column", "resolution")
        resolution = self.attrs.get("resolution", [720, 1280])
        row_num = self.attrs.get("row_num", 1)

        timeline_table[project_model_builder_column] = None
        timeline_table[input_file_map_column] = None
        timeline_table[resolution_column] = None
        for index in range(row_num):
            project_model_builder = EditSceneVideoProjectBuilder()

            if timeline_table.shape[0] <= index:
                timeline_table.loc[index] = None
            timeline_table.at[index, project_model_builder_column] = project_model_builder
            timeline_table.at[index, input_file_map_column] = {}
            timeline_table.at[index, resolution_column] = resolution

        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(CreateMinecraftRequestNewOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="project_model_builder_column", type="str", desc="项目模型构建列名") \
    .add_attr(name="input_file_map_column", type="str", desc="输入文件映射列名") \
    .add_attr(name="resolution_column", type="str", desc="分辨率列名") \
    .add_attr(name="resolution", type="list", desc="最终视频分辨率") \
    .add_attr(name="row_num", type="int", desc="创建行数")
