import glob
import math
import os

from video_graph.common.utils.logger import logger
from video_graph.common.utils.tools import video_split
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoSplitOp(Op):
    """
    Function:
        将长时间视频切分成若干个短视频，输出一个新表，存储切分后的视频信息

    Attributes:
        input:
        video_file_path_column (str, optional): 视频本地路径所在列名, 默认为 'video_file_path'
        video_duration_column (str, optional): 视频时长列名，默认为 'duration'
        video_segment_duration (int, optional): 每切片时长 (单位为秒), 默认为 10s
        video_name_column (str, optional): 原始视频名, 后续 merge 可能会用, 默认为 'video_name'

        output:
        video_name_column (str, optional): 原始视频名, 后续 merge 可能会用, 默认为 'video_name'
        video_segment_path_column (str, optional): 切片路径所在列, 默认为 'video_segment_path'
        video_segment_index_column (str, optional): 视频切片索引列名，默认为 'video_segment_index'

    InputTables:
        material_table: 完整视频所在的表格

    OutputTables:
        video_segment_table: 切分完的视频数据输出表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_process_op/video_split_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.video_process_op.video_split_op import *

        # 创建输入表格
        #video_name用于标记每个切片对应的原视频，从而合并属于同原视频的所有切片
        input_table = DataTable(
            name="TestTable",
            data = {
                "video_file_path":["test_op/file.mp4"],
                "duration":[6.8],
                "video_name":["file.mp4"],
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        video_split_op = VideoSplitOp(
            name="VideoSplitOp",
            attrs={
                "video_file_path_column":"video_file_path",
                "video_duration_column":"duration",
                "video_segment_duration": 3,
                "video_name_column":"video_name",
                "video_segment_path_column": "video_segment_path",
                "video_segment_index_column": "video_segment_index"
            }
        )

        # 执行算子
        success = video_split_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        video_file_path_column = self.attrs.get('video_file_path_column', 'video_file_path')
        video_duration_column = self.attrs.get('video_duration_column', 'duration')
        video_segment_duration = self.attrs.get('video_segment_duration', 10)
        video_name_column = self.attrs.get('video_name_column', 'video_name')

        video_name_list = []
        all_segment_path_list = []
        video_segment_index_list = []

        video_index = 0
        for index, row in material_table.iterrows():
            video_index += 1
            video_file_path = row.get(video_file_path_column)
            video_duration = row.get(video_duration_column)
            video_name = row.get(video_name_column)
            if not video_file_path or not os.path.exists(video_file_path) or not video_duration or video_duration == 0:
                logger.info(f'invalid video meta info, video path {video_file_path},'
                            f'video duration {video_duration}')
                continue

            if not video_split(video_file_path, video_segment_duration):
                logger.error('fail to split video')
                continue

            expected_segment_num = math.ceil(video_duration / video_segment_duration)
            video_dir_path = '.' if len(video_file_path.split('/')) == 1 else '/'.join(video_file_path.split('/')[:-1])
            file_name = video_file_path.split('/')[-1].split('.')[0]
            file_format = video_file_path.split('/')[-1].split('.')[-1]
            segment_path_list = glob.glob(f'{video_dir_path}/{file_name}-*.{file_format}')
            segment_path_list.sort()
            if expected_segment_num != len(segment_path_list):
                logger.error('unexpected video segment number after video split')
                continue

            video_name_list.extend([video_name] * len(segment_path_list))
            all_segment_path_list.extend(segment_path_list)
            video_segment_index_list.extend([f'{video_index}_{segment_idx}'
                                             for segment_idx in range(len(segment_path_list))])

        video_segment_path_column = self.attrs.get('video_segment_path_column', 'video_segment_path')
        video_segment_index_column = self.attrs.get('video_segment_index_column', 'video_segment_index')
        video_segment_table = DataTable(name='video_segment_table', data={
            video_name_column: video_name_list,
            video_segment_path_column: all_segment_path_list,
            video_segment_index_column: video_segment_index_list
        })
        op_context.output_tables.append(video_segment_table)
        return True


op_register.register_op(VideoSplitOp) \
    .add_input(name='material_table', type='DataTable', desc='素材表') \
    .add_output(name='video_segment_table', type='DataTable', desc='视频切片表') \
    .add_attr(name='video_file_path_column', type='str', desc='视频本地路径所在列名') \
    .add_attr(name='video_duration_column', type='str', desc='视频时长列名') \
    .add_attr(name='video_segment_duration', type='int', desc='每切片时长 (单位为秒)') \
    .add_attr(name='video_name_column', type='str', desc='原始视频名, 后续如 merge 可能会用') \
    .add_attr(name='video_segment_path_column', type='str', desc='切片路径所在列') \
    .add_attr(name='video_index_column', type='str', desc='视频切片索引列名') \
    .set_parallel(True)
