import pandas as pd
import struct
import time
from math import isnan

# dnt_file_path = "itemdropgrouptable.dnt"
# csv_file_path = "itemdropgrouptable.csv"
# dnt_file_path = "stagerewardtable.dnt"
# csv_file_path = "stagerewardtable.csv"
# dnt_file_path = "stagerewardtable_normal.dnt"
# csv_file_path = "stagerewardtable_normal.csv"
dnt_file_path = "itemtable_equipment.dnt"
csv_file_path = "itemtable_equipment.csv"


def ReadDataToDF(file_path):
    '''
    读取file_path所对应的二进制dnt文件，并将其输出为DataFrame数据结构
    dnt_bytes:
    columns:
    rows:
    seek:
    column_name_size:
    column_name:
    column_arg_type:
    '''
    begin_time = time.time()
    with open(file_path, 'rb')as dnt_handle:
        dnt_bytes = dnt_handle.read()
        columns = struct.unpack('H', dnt_bytes[4:6])[0]
        columns = columns + 1  # 加上_RowID
        rows = struct.unpack('I', dnt_bytes[6:10])[0]
        seek = 10
        data_frame = pd.DataFrame()
        column_info = [{'column_name_size': 6,
                        'column_name': '_RowID', 'column_arg_type': 3}]
        data_frame['_RowID'] = None
        for column in range(1, columns):
            column_info_item = {}
            column_info_item['column_name_size'] = struct.unpack(
                'B', dnt_bytes[seek:seek+1])[0]
            seek = seek+2
            column_info_item['column_name'] = str(struct.unpack(
                str(column_info_item['column_name_size'])+'s',
                dnt_bytes[seek:seek+column_info_item['column_name_size']])[0], encoding="utf8")
            # print(column_info_item['column_name'])
            seek = seek + column_info_item['column_name_size']
            column_info_item['column_arg_type'] = struct.unpack(
                'B', dnt_bytes[seek:seek+1])[0]
            seek = seek+1
            data_frame[column_info_item['column_name']] = None
            column_info.append(column_info_item)
        before_read_data = time.time()
        data_frame_result = ReadData(
            data_frame, column_info, rows, dnt_bytes, seek)
        end_read_data = time.time()
        print("before read data time cost:"+str(before_read_data-begin_time))
        print("read data time cost:"+str(end_read_data-before_read_data))
        return (data_frame_result, column_info)


def FillDataType(column_info_item, dnt_bytes, seek):
    '''
    向column_info_item里填充参数数据
    seek:对应dnt_bytes中正在处理的位置
    return: seek或0(即失败)
    '''
    if column_info_item['column_arg_type'] == 1:
        column_info_item['column_arg_size'] = struct.unpack(
            'H', dnt_bytes[seek:seek+2])[0]
        seek = seek+2
        column_info_item['column_arg_data'] = str(struct.unpack(
            str(column_info_item['column_arg_size'])+'s',
            dnt_bytes[seek:seek+column_info_item['column_arg_size']])[0], encoding="utf8")
        seek = seek + column_info_item['column_arg_size']
    elif (column_info_item['column_arg_type'] == 2 or column_info_item['column_arg_type'] == 3):
        column_info_item['column_arg_size'] = 4
        column_info_item['column_arg_data'] = struct.unpack(
            'I', dnt_bytes[seek:seek+4])[0]
        seek = seek+4
    elif column_info_item['column_arg_type'] == 4:
        column_info_item['column_arg_size'] = 4
        column_info_item['column_arg_data'] = struct.unpack(
            'f', dnt_bytes[seek:seek+4])[0]/100
        seek = seek+4
    elif column_info_item['column_arg_type'] == 5:
        column_info_item['column_arg_size'] = 4
        column_info_item['column_arg_data'] = struct.unpack(
            'f', dnt_bytes[seek:seek+4])[0]
        seek = seek+4
    else:
        return 0
    return seek


def WriteDataType(output_data, arg_data, arg_type):
    '''
    根据arg_type将arg_data填充进output_data
    return: 新的output_data
    '''
    if arg_type == 1:
        if not isinstance(arg_data,str) and isnan(arg_data): 
            arg_len = 0
            output_data += struct.pack('H', arg_len)
        else:
            arg_len = len(str(arg_data))
            output_data += struct.pack('H', arg_len)
            output_data += struct.pack(str(arg_len)+'s', str.encode(str(arg_data)))
    elif (arg_type == 2 or arg_type == 3):
        output_data += struct.pack('I', int(arg_data))
    elif arg_type == 4:
        output_data += struct.pack('f', arg_data*100)
    elif arg_type == 5:
        output_data += struct.pack('f', arg_data)
    else:
        return 0
    return output_data


def ReadData(data_frame, column_info, rows, dnt_bytes, seek):
    '''
    读取所有剩余的数据信息
    data_frame:初始数据集
    return: 新的数据集或0(error)
    '''
    data_frame_append_timecost = 0
    fill_data_timecost = 0
    init_series_timecost = 0
    assign_data_timecost = 0
    for row in range(0, rows):
        before_init_series = time.time()
        row_item = {}
        end_init_series = time.time()
        init_series_timecost += (end_init_series - before_init_series)
        for column_info_item in column_info:
            before_fill_data = time.time()
            seek_tmp = FillDataType(column_info_item, dnt_bytes, seek)
            end_fill_data = time.time()
            fill_data_timecost += (end_fill_data - before_fill_data)
            before_assign_data = time.time()
            row_item[column_info_item['column_name']
                     ] = column_info_item['column_arg_data']
            end_assign_data = time.time()
            assign_data_timecost += (end_assign_data-before_assign_data)
            if seek_tmp != 0:
                seek = seek_tmp
            else:
                print('error! row ='+row+' data:\n')
                print(column_info_item)
                return None
        before_append_data = time.time()
        data_frame = data_frame.append(row_item, ignore_index=True)
        end_append_data = time.time()
        data_frame_append_timecost += (end_append_data-before_append_data)
    print('data frame append time cost : '+str(data_frame_append_timecost))
    print('fill data time cost : '+str(fill_data_timecost))
    print('init series time cost : '+str(init_series_timecost))
    print('assign data time cost : '+str(assign_data_timecost))
    return data_frame


def WriteData(data_frame, column_info, output_dnt_file):
    '''
    将data_frame对应的数据保存到output_dnt_file
    column_info : dataframe 类型的数据
    '''
    print(column_info)
    rows = data_frame.shape[0]
    columns = data_frame.shape[1]-1
    output_data = ''
    with open(output_dnt_file, 'wb')as dnt_handle:
        output_data = struct.pack('I', 0)  # 开头4字节
        output_data += struct.pack('H', columns)  # 列
        output_data += struct.pack('I', rows)  # 行
        index = 0
        for column in data_frame.columns:
            if column != '_RowID':
                col_len = len(column)
                output_data += struct.pack('B', col_len)
                output_data += struct.pack('B', 0)
                output_data += struct.pack(str(col_len) +
                                           's', str.encode(column))
                output_data += struct.pack('B',
                                           column_info['column_arg_type'][index])
            index = index + 1

        for row in data_frame.iterrows():
            index = 0
            for column in row[1]:
                output_data = WriteDataType(
                    output_data, column, column_info['column_arg_type'][index])
                index += 1

        dnt_handle.write(output_data)


def WriteToCSV(data_frame, csv_file_name):
    data_frame.to_csv(csv_file_name)


def ConvertDntToCSV(dnt_file_name, csv_file_name):
    read_data_result = ReadDataToDF(dnt_file_name)
    data_frame = read_data_result[0]
    info_frame = read_data_result[1]
    WriteToCSV(data_frame, csv_file_name)
    WriteToCSV(pd.DataFrame(info_frame), 'info_'+csv_file_name)


def ConvertCSVToDnt(csv_file_name, dnt_file_name):
    before_convert_time = time.time()
    data_frame = pd.read_csv(csv_file_name, index_col=0)
    info_data_frame = pd.read_csv('info_'+csv_file_name, index_col=0)
    WriteData(data_frame, info_data_frame, dnt_file_name)
    end_convert_time = time.time()
    print("conver csv to dnt time cost : " +
          str(end_convert_time-before_convert_time))


if __name__ == '__main__':
    ConvertDntToCSV(dnt_file_path, csv_file_path)
    ConvertCSVToDnt(csv_file_path, "test"+dnt_file_path)
