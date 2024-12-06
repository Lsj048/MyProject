import os

from video_graph.common.utils.blobstore import BlobStoreClientManager
from video_graph.common.utils.logger import logger
from video_graph.common.utils.tools import parse_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class FileDownloaderOp(Op):
    """
    Function:
        文件下载算子，用于下载 Blob 存储中的文件到本地

    Attributes:
        file_blob_key_column (str): 输入表格中存储文件 Blob Key 的列名。
        file_path_column (str): 输入表格中保存文件路径的列名。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        in_table: 添加了文件路径的输入表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/file_io_op/file_downloader_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.file_io_op.file_downloader_op import *

        #创建输入表数据
        input_table = DataTable(
            name = "TestTable",
            data = {
            "file_blob_key":["ad_nieuwland-material_"]
        })
        # 创建OpContext并添加输入表
        op_context = OpContext("graph_name","request_tag","request_id")
        op_context.input_tables.append(input_table)

        # 创建并运行FileDownloadOp实例，，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
        file_downloader_op = FileDownloaderOp(
            name="FileDownloaderOp",
            attrs={
            "file_blob_key_column": "file_blob_key",
        })
        success = file_downloader_op.process(op_context)

        # 检查结果
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子计算失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        file_blob_key_column = self.attrs.get("file_blob_key_column", "file_blob_key")
        file_path_column = self.attrs.get("file_path_column", "file_path")
        filename_prefix = f"{op_context.request_id}-{op_context.thread_id}"
        file_directory = f"{op_context.process_id}"

        in_table[file_path_column] = None
        for index, row in in_table.iterrows():
            file_blob_key = row.get(file_blob_key_column)
            if str(file_blob_key) == 'nan' or file_blob_key is None:
                continue
            db, table, key = parse_bbs_resource_id(file_blob_key)
            target_key = os.path.basename(key)
            file_name = f"{filename_prefix}-{target_key}"
            file_path = os.path.join(file_directory, file_name)
            blob_client = BlobStoreClientManager().get_client(f"{db}-{table}")
            status = blob_client.download_file_with_retry(key, file_path)
            if status is False:
                logger.error(f"file_blob_key:{file_blob_key} download failed")
                continue

            in_table.loc[index, file_path_column] = file_path

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(FileDownloaderOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="file_blob_key_column", type="str", desc="blobstore地址列名") \
    .add_attr(name="file_path_column", type="str", desc="文件地址列名") \
    .set_parallel(True)
