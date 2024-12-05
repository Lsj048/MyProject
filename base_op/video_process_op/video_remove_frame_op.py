from video_graph.common.utils.tools import video_remove_frame
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoRemoveFrameOp(Op):
    """
    【local】移除视频中的某一帧，得到一个新视频文件

    Attributes:
        input_file_column (str): 输入视频文件路径所在的列名，默认为"video_file_path"
        output_file_column (str): 输出视频文件路径所在的列名，默认为"output_video_file_path"
        frame_index (int): 需要移除的帧的索引，默认为0

    InputTables:
        video_table: 视频文件所在的表格

    OutputTables:
        video_table: 添加了输出视频文件路径的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_process_op/video_remove_frame_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.video_process_op.video_remove_frame_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                "video_file_path":["test_op/file.mp4"],
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        video_remove_frame_op = VideoRemoveFrameOp(
            name="VideoRemoveFrameOp",
            attrs={
                "input_file_column":"video_file_path",
                "output_file_column":"output_video_file_path",
                "frame_index": 0
            }
        )

        # 执行算子
        success = video_remove_frame_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        video_table: DataTable = op_context.input_tables[0]
        input_file_column = self.attrs.get("input_file_column", "video_file_path")
        output_file_column = self.attrs.get("output_file_column", "output_video_file_path")
        frame_index = self.attrs.get("frame_index", 0)

        video_table[output_file_column] = None
        for index, row in video_table.iterrows():
            video_file_path = row.get(input_file_column)
            base_file, ext = video_file_path.split('.')
            output_video = f"{base_file}-remove-frame.{ext}"
            video_remove_frame(video_file_path, output_video, frame_index)

            video_table.at[index, output_file_column] = output_video

        op_context.output_tables.append(video_table)
        return True


op_register.register_op(VideoRemoveFrameOp) \
    .add_input(name="video_table", type="DataTable", desc="视频表") \
    .add_output(name="video_table", type="DataTable", desc="视频表")
