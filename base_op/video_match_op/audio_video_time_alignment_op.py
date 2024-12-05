from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AudioVideoTimeAlignmentOp(Op):
    """
    【local】音频与视频时间对齐，为选用的视频添加开始时间和结束时间

    Attributes:
        audio_duration_column (str): 音频时长列名，默认为"audio_duration"
        video_tuple_column (str): 视频元组列名，默认为"video_tuple"
        start_time_column (str): 开始时间列名，默认为"start_time"
        end_time_column (str): 结束时间列名，默认为"end_time"

    InputTables:
        material_table: 素材表
        shot_table: 镜号表

    OutputTables:
        material_table: 添加了开始时间和结束时间的素材表

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_match_op/audio_video_time_alignment_op.py?ref_type=heads

    Examples:
        #等待作者更新～
    """

    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        shot_table: DataTable = op_context.input_tables[1]
        audio_duration_column = self.attrs.get("audio_duration_column", "audio_duration")
        video_tuple_column = self.attrs.get("video_tuple_column", "video_tuple")
        start_time_column = self.attrs.get("start_time_column", "start_time")
        end_time_column = self.attrs.get("end_time_column", "end_time")

        total_duration = 0
        for index, row in shot_table.iterrows():
            audio_duration = row.get(audio_duration_column)
            total_duration += audio_duration

        remain_duration = total_duration
        material_table[start_time_column] = 0.0
        material_table[end_time_column] = 0.0
        for index, row in material_table.iterrows():
            start_time, end_time, video_blob_key = row.get(video_tuple_column)
            duration = end_time - start_time
            if duration > remain_duration:
                material_table.at[index, start_time_column] = start_time
                material_table.at[index, end_time_column] = start_time + remain_duration
                remain_duration = 0
                break
            else:
                material_table.at[index, start_time_column] = start_time
                material_table.at[index, end_time_column] = end_time
                remain_duration -= duration

        if remain_duration > 0:
            self.fail_reason = (f"视频时长太短，文本时长：{total_duration}，视频个数：{material_table.shape[0]}，"
                                f"时长差距：{remain_duration}")
            self.trace_log.update({"fail_reason": self.fail_reason})
            return False

        op_context.output_tables.append(material_table)
        return True


op_register.register_op(AudioVideoTimeAlignmentOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="audio_duration_column", type="str", desc="音频时长列名") \
    .add_attr(name="video_tuple_column", type="str", desc="视频元组列名") \
    .add_attr(name="start_time_column", type="str", desc="开始时间列名") \
    .add_attr(name="end_time_column", type="str", desc="结束时间列名")
