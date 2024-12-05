from video_graph.common.utils.tools import video_merge
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoMergeOp(Op):
    """
    【local】根据视频下标列做聚合, 将多个视频切片合并成一个视频。
    其中，视频下表列的格式应为 {video_index}_{segment_index}, 同一 video_index 的多个切片按照 segment_index 顺序合并

    Attributes:
        input:
        video_segment_path_column (str, optional): 视频切片路径所在列, 默认为 'video_segment_path'
        video_segment_index_column (str, optional): 视频切片索引列名，默认为 'video_segment_index'
        video_name_column (str, optional): 切片对应原始视频名, 默认为 'video_name'
        check_video_segment_valid (bool, optional): 是否校验视频片段的合法性, 默认为 False
        video_segment_status_column (str, optional): 视频合法性标识所在列, 默认为 'video_segment_status'

        output:
        video_name_column (str, optional): 聚合后视频对应原始视频名, 默认为 'video_name'
        merged_video_path_column (str, optional): 聚合后的视频所在列, 默认为 'merged_video_path'

    InputTables:
        video_segment_table: 视频切片所在表格

    OutputTables:
        material_table: 完整视频所在表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_process_op/video_merge_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.video_process_op.video_merge_op import *

        # 创建输入表格
        #video_segment_path为视频切片文件路径
        #video_segment_index确定合并视频的顺序
        #video_name用于标记每个切片对应的原视频，从而合并属于同原视频的所有切片
        input_table = DataTable(
            name="TestTable",
            data = {
                "video_segment_path":["test_op/test_video_file.mp4","test_op/test_video_file2.mp4"],
                "video_segment_index":["video_1","video_2"],
                "video_name":["test_video_file.mp4","test_video_file.mp4"],
                "video_segment_status":[True,True]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        video_merge_op = VideoMergeOp(
            name="VideoMergeOp",
            attrs={
                "video_segment_path_column":"video_segment_path",
                "video_segment_index_column":"video_segment_index",
                "check_video_segment_valid": False,
                "video_name_column":"video_name",
                "video_segment_status_column":"video_segment_status",
                "merge_video_name_column":"merge_video_name",
                "merged_video_path_column":"merged_video_path"
            }
        )

        # 执行算子
        success = video_merge_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        video_segment_table = op_context.input_tables[0]
        video_segment_path_column = self.attrs.get('video_segment_path_column', 'video_segment_path')
        video_segment_index_column = self.attrs.get('video_segment_index_column', 'video_segment_index')
        video_name_column = self.attrs.get('video_name_column', 'video_name')
        check_video_segment_valid = self.attrs.get('check_video_segment_valid', False)
        video_segment_status_column = self.attrs.get('video_segment_status_column', 'video_segment_status')

        video_name_to_segment = {}
        video_name_to_valid_info = {}
        origin_video_name_list = []
        for index, row in video_segment_table.iterrows():
            video_segment_path = row.get(video_segment_path_column)
            video_segment_index = row.get(video_segment_index_column)
            video_name = row.get(video_name_column)

            if video_name in video_name_to_segment:
                segment_index = int(video_segment_index.split('_')[1])
                if segment_index >= len(video_name_to_segment[video_name]):
                    video_name_to_segment[video_name].append(video_segment_path)
                else:
                    video_name_to_segment[video_name].insert(segment_index, video_segment_path)
            else:
                video_name_to_segment[video_name] = [video_segment_path]

            if check_video_segment_valid:
                if video_name in video_name_to_valid_info:
                    video_name_to_valid_info[video_name] &= row.get(video_segment_status_column)
                else:
                    video_name_to_valid_info[video_name] = row.get(video_segment_status_column)
            else:
                video_name_to_valid_info[video_name] = True

        merge_success_video_path = []
        for video_name in video_name_to_segment:
            if not video_name_to_valid_info[video_name]:
                continue
            output_video_name = video_name.split('/')[-1].split('_')[-1]
            output_path = '/'.join(video_name_to_segment[video_name][0].split('/')[:-1])
            if video_merge(output_path, video_name_to_segment[video_name], output_video_name,
                           f'{op_context.request_id}-{op_context.thread_id}-'):
                merge_success_video_path.append(f'{output_path}/{output_video_name}')
                origin_video_name_list.append(video_name)

        merged_video_path_column = self.attrs.get('merged_video_path_column', 'merged_video_path')
        material_table = DataTable(name='material_table', data={
            merged_video_path_column: merge_success_video_path,
            video_name_column: origin_video_name_list
        })
        op_context.output_tables.append(material_table)
        return True


op_register.register_op(VideoMergeOp) \
    .add_input(name='video_segment_table', type='DataTable', desc='视频切片所在表格') \
    .add_output(name='material_table', type='DataTable', desc='完整视频所在表格') \
    .add_attr(name='video_segment_path_column', type='str', desc='视频切片路径所在列') \
    .add_attr(name='video_segment_index_column', type='str', desc='视频切片索引列名') \
    .add_attr(name='video_name_column', type='str', desc='切片对应原始视频名') \
    .add_attr(name='check_video_segment_valid', type='bool', desc='是否校验视频片段的合法性') \
    .add_attr(name='video_segment_status_column', type='str', desc='视频合法性标识所在列') \
    .add_attr(name='merged_video_path_column', type='str', desc='聚合后的视频所在列')
