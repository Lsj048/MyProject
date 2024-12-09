from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class CollectVtuberHumanAudioOp(Op):
    """
    Function:
        收集虚拟人音频算子，组成完整时间线

    Attributes:
        vtuber_name_column (str): 虚拟人名称列名，默认为"vtuber_name"。
        audio_file_column (str): 音频文件列名，默认为"audio_file"。
        silent_audio_file_column (str): 静音音频文件列名，默认为"silent_audio_file"。
        copy_columns (List[str]): 需要复制的列名，默认为["vtuber_id", "vtuber_age", "vtuber_gender"]。

    InputTables:
        shot_table: 输入表格。
        vtuber_table: 虚拟人表格。

    OutputTables:
        vtuber_table: 添加了虚拟人音频的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/extend_op/vtuber_human_op/collect_vtuber_human_audio_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        vtuber_table: DataTable = op_context.input_tables[1]
        vtuber_name_column = self.attrs.get("vtuber_name_column", "vtuber_name")
        audio_file_column = self.attrs.get("audio_file_column", "audio_file")
        silent_audio_file_column = self.attrs.get("silent_audio_file_column", "silent_audio_file")
        copy_columns = self.attrs.get("copy_columns", ["vtuber_id", "vtuber_age", "vtuber_gender"])

        vtuber_name_list = []
        for index, row in shot_table.iterrows():
            vtuber_name = row.get(vtuber_name_column)
            if vtuber_name and vtuber_name not in vtuber_name_list:
                vtuber_name_list.append(vtuber_name)
        vtuber_name_to_info = {}
        vtuber_info_keys = ["vtuber_name", "audio_file_list"] + copy_columns
        for target_vtuber_name in vtuber_name_list:
            audio_list = []
            extra_column = {}
            for index, row in shot_table.iterrows():
                vtuber_name = row.get(vtuber_name_column)
                if target_vtuber_name == vtuber_name:
                    audio_list.append(row.get(audio_file_column))
                    for copy_column in copy_columns:
                        col = row.get(copy_column)
                        extra_column.update({copy_column: col})
                else:
                    audio_list.append(row.get(silent_audio_file_column))
            vtuber_info = {
                "vtuber_name": target_vtuber_name,
                "audio_file_list": audio_list
            }
            vtuber_info.update(extra_column)
            vtuber_name_to_info.update({target_vtuber_name: vtuber_info})
        for col in vtuber_info_keys:
            if col not in vtuber_table.columns:
                vtuber_table[col] = None
        for vtuber_name, vtuber_info in vtuber_name_to_info.items():
            vtuber_table.loc[len(vtuber_table)] = vtuber_info

        op_context.output_tables.append(vtuber_table)
        return True


op_register.register_op(CollectVtuberHumanAudioOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="vtuber_table", type="DataTable", desc="虚拟人表") \
    .add_output(name="vtuber_table", type="DataTable", desc="虚拟人表")
