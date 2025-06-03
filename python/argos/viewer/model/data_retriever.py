import zlib, struct, copy, re
from enum import IntEnum
from viewer.gui.view_settings import DirtyReasons

class DataRetriever:
    def __init__(self, frame, db, simhier):
        self.frame = frame
        self._db = db
        self.simhier = simhier
        self.cursor = db.cursor()
        cursor = self.cursor

        cursor.execute('SELECT Heartbeat FROM CollectionGlobals')
        self._heartbeat = cursor.fetchone()[0]

        cursor.execute('SELECT DISTINCT(Tick) FROM CollectionRecords ORDER BY Tick ASC')
        self._time_vals = [row[0] for row in cursor.fetchall()]

        self._displayed_columns_by_struct_name = {}
        self._auto_colorize_column_by_struct_name = {}
        self._cached_utiliz_sizes = {}
        self._cached_utiliz_time_val = None

        self._collection_ids_by_elem_path = {}
        for elem_path in simhier.GetElemPaths():
            collection_id = simhier.GetCollectionID(elem_path)
            if collection_id:
                self._collection_ids_by_elem_path[elem_path] = collection_id

        cursor.execute('SELECT IntVal,String FROM StringMap')
        strings_by_int = {}
        for intval,stringval in cursor.fetchall():
            strings_by_int[intval] = stringval

        cursor.execute('SELECT EnumName,EnumValStr,EnumValBlob,IntType FROM EnumDefns')
        enums_by_name = {}
        for enum_name,enum_str,enum_blob,int_type in cursor.fetchall():
            if enum_name not in enums_by_name:
                enums_by_name[enum_name] = EnumDef(enum_name, int_type)

            enums_by_name[enum_name].AddEntry(enum_str, enum_blob)

        cursor.execute('SELECT StructName,FieldType FROM StructFields')
        struct_num_bytes_by_struct_name = {}
        for struct_name, field_type in cursor.fetchall():
            if struct_name not in struct_num_bytes_by_struct_name:
                struct_num_bytes_by_struct_name[struct_name] = 0

            if field_type in ('char', 'int8_t', 'uint8_t'):
                struct_num_bytes_by_struct_name[struct_name] += 1
            elif field_type in ('int16_t', 'uint16_t'):
                struct_num_bytes_by_struct_name[struct_name] += 2
            elif field_type in ('int32_t', 'uint32_t', 'float_t'):
                struct_num_bytes_by_struct_name[struct_name] += 4
            elif field_type in ('int64_t', 'uint64_t', 'double_t'):
                struct_num_bytes_by_struct_name[struct_name] += 8
            elif field_type == 'bool':
                struct_num_bytes_by_struct_name[struct_name] += 4 # bools are stored as int32_t
            elif field_type == 'string_t':
                struct_num_bytes_by_struct_name[struct_name] += 4 # strings are stored as uint32_t
            elif field_type in enums_by_name:
                struct_num_bytes_by_struct_name[struct_name] += enums_by_name[field_type].GetNumBytes()
            else:
                raise ValueError('Invalid field type: ' + field_type)

        self._deserializers_by_dtype = {}
        pod_dtypes = ('char', 'int8_t', 'int16_t', 'int32_t', 'int64_t', 'uint8_t', 'uint16_t', 'uint32_t', 'uint64_t', 'float_t', 'double_t', 'bool')
        for dtype in pod_dtypes:
            self._deserializers_by_dtype[dtype] = PODDeserializer(dtype)

        for dtype in enums_by_name.keys():
            self._deserializers_by_dtype[dtype] = EnumDeserializer(enums_by_name[dtype])

        self._deserializers_by_dtype['string_t'] = StringDeserializer(strings_by_int)

        for struct_name in struct_num_bytes_by_struct_name.keys():
            deserializer = StructDeserializer(self, struct_name, strings_by_int, enums_by_name)
            cmd = 'SELECT FieldName,FieldType,FormatCode FROM StructFields WHERE StructName="{}"'.format(struct_name)
            cursor.execute(cmd)

            for field_name, field_type, format_code in cursor.fetchall():
                field_deserializer = self._deserializers_by_dtype[field_type]
                deserializer.AddField(field_name, field_deserializer, format_code)

            self._deserializers_by_dtype[struct_name] = deserializer

        cursor.execute('SELECT ElementTreeNodeID,DataType,AutoCollected FROM CollectableTreeNodes')
        self._replayers_by_elem_path = {}
        self._dtypes_by_elem_path = {}
        self._auto_collected_cids = set()
        for id, dtype, auto_collected in cursor.fetchall():
            elem_path = simhier.GetElemPath(id)
            self._dtypes_by_elem_path[elem_path] = dtype

            if auto_collected:
                self._auto_collected_cids.add(id)

            if dtype in ('char', 'int8_t', 'int16_t', 'int32_t', 'int64_t', 'uint8_t', 'uint16_t', 'uint32_t', 'uint64_t', 'float_t', 'double_t', 'bool'):
                self._replayers_by_elem_path[elem_path] = PODReplayer(dtype)
            elif dtype == 'string_t':
                self._replayers_by_elem_path[elem_path] = StringReplayer(strings_by_int)
            elif dtype in enums_by_name:
                self._replayers_by_elem_path[elem_path] = EnumReplayer(dtype, enums_by_name)
            elif '_contig_capacity' in dtype:
                pattern = re.compile(r'(.*)_contig_capacity(\d+)')
                match = pattern.match(dtype)
                if match is None:
                    raise ValueError('Invalid data type: ' + dtype)
                
                struct_name = match.group(1)
                capacity = int(match.group(2))
                struct_num_bytes = struct_num_bytes_by_struct_name[struct_name]
                self._replayers_by_elem_path[elem_path] = ContigIterableReplayer(struct_num_bytes, capacity)
            elif '_sparse_capacity' in dtype:
                pattern = re.compile(r'(.*)_sparse_capacity(\d+)')
                match = pattern.match(dtype)
                if match is None:
                    raise ValueError('Invalid data type: ' + dtype)
                
                struct_name = match.group(1)
                capacity = int(match.group(2))
                struct_num_bytes = struct_num_bytes_by_struct_name[struct_name]
                self._replayers_by_elem_path[elem_path] = SparseIterableReplayer(struct_num_bytes, capacity)
            else:
                struct_name = dtype
                struct_num_bytes = struct_num_bytes_by_struct_name[struct_name]
                self._replayers_by_elem_path[elem_path] = ScalarStructReplayer(struct_name, struct_num_bytes)

    def IsDevDebug(self):
        return self.frame.dev_debug

    def GetCurrentViewSettings(self):
        settings = {}
        assert set(self._displayed_columns_by_struct_name.keys()) == set(self._auto_colorize_column_by_struct_name.keys())

        for struct_name,displayed_columns in self._displayed_columns_by_struct_name.items():
            assert len(displayed_columns) > 0
            settings[struct_name] = {'auto_colorize_column':None}
            settings[struct_name]['displayed_columns'] = copy.deepcopy(displayed_columns)
        
        for struct_name,auto_colorize_column in self._auto_colorize_column_by_struct_name.items():
            assert struct_name in settings
            settings[struct_name]['auto_colorize_column'] = None if not auto_colorize_column else auto_colorize_column

        return settings
    
    def ApplyViewSettings(self, settings):
        self._displayed_columns_by_struct_name = {}
        self._auto_colorize_column_by_struct_name = {}

        for struct_name,struct_settings in settings.items():
            displayed_columns = struct_settings['displayed_columns']
            auto_colorize_column = struct_settings['auto_colorize_column']

            self._displayed_columns_by_struct_name[struct_name] = copy.deepcopy(displayed_columns)
            self._auto_colorize_column_by_struct_name[struct_name] = auto_colorize_column

        self.frame.inspector.RefreshWidgetsOnAllTabs()

    def GetCurrentUserSettings(self):
        # All our settings are in the user settings and do not affect the view file
        return {}

    def ApplyUserSettings(self, settings):
        # All our settings are in the user settings and do not affect the view file
        pass

    def ResetToDefaultViewSettings(self, update_widgets=True):
        self.cursor.execute('SELECT DISTINCT(StructName) FROM StructFields')
        struct_names = []
        for struct_name in self.cursor.fetchall():
            struct_names.append(struct_name[0])

        self._auto_colorize_column_by_struct_name = {}
        self._displayed_columns_by_struct_name = {}

        for struct_name in struct_names:
            cmd = 'SELECT FieldName FROM StructFields WHERE StructName="{}" AND IsAutoColorizeKey=1'.format(struct_name)
            self.cursor.execute(cmd)
            auto_colorize_column = [row[0] for row in self.cursor.fetchall()]
            assert len(auto_colorize_column) <= 1
            if len(auto_colorize_column) == 1:
                self._auto_colorize_column_by_struct_name[struct_name] = auto_colorize_column[0]
            else:
                self._auto_colorize_column_by_struct_name[struct_name] = None

            cmd = 'SELECT FieldName FROM StructFields WHERE StructName="{}" AND IsDisplayedByDefault=1'.format(struct_name)
            self.cursor.execute(cmd)
            displayed_columns = [row[0] for row in self.cursor.fetchall()]
            assert len(displayed_columns) > 0
            self._displayed_columns_by_struct_name[struct_name] = displayed_columns

        if update_widgets:
            self.frame.inspector.RefreshWidgetsOnAllTabs()

    def SetVisibleFieldNames(self, elem_path, field_names):
        deserializer = self.GetDeserializer(elem_path)
        struct_name = deserializer.struct_name
        assert struct_name in self._displayed_columns_by_struct_name

        if self._displayed_columns_by_struct_name[struct_name] == field_names:
            return

        auto_colorize_col = self.GetAutoColorizeColumn(elem_path)
        if auto_colorize_col not in field_names:
            self.SetAutoColorizeColumn(elem_path, None)

        self._displayed_columns_by_struct_name[struct_name] = copy.deepcopy(field_names)
        self.frame.inspector.RefreshWidgetsOnAllTabs()
        self.frame.view_settings.SetDirty(reason=DirtyReasons.QueueTableDispColsChanged)

    def SetAutoColorizeColumn(self, elem_path, field_name):
        deserializer = self.GetDeserializer(elem_path)
        struct_name = deserializer.struct_name
        assert struct_name in self._auto_colorize_column_by_struct_name

        if self._auto_colorize_column_by_struct_name[struct_name] == field_name:
            return

        self._auto_colorize_column_by_struct_name[struct_name] = field_name
        self.frame.inspector.RefreshWidgetsOnAllTabs()
        self.frame.view_settings.SetDirty(reason=DirtyReasons.QueueTableAutoColorizeChanged)

    def GetAutoColorizeColumn(self, elem_path):
        deserializer = self.GetDeserializer(elem_path)
        struct_name = deserializer.struct_name
        return self._auto_colorize_column_by_struct_name.get(struct_name)

    def GetDeserializer(self, elem_path):
        dtype = self._dtypes_by_elem_path[elem_path]
        if dtype not in self._deserializers_by_dtype:
            if '_contig_capacity' in dtype:
                dtype = dtype.split('_contig_capacity')[0]
            elif '_sparse_capacity' in dtype:
                dtype = dtype.split('_sparse_capacity')[0]
            else:
                raise ValueError('Invalid data type: ' + dtype)

        return self._deserializers_by_dtype[dtype]

    def GetIterableSizesByCollectionID(self, time_val):
        if self._cached_utiliz_time_val is not None and time_val == self._cached_utiliz_time_val:
            return self._cached_utiliz_sizes

        return {id:0 for id in self.simhier.GetContainerIDs()}

    def Unpack(self, elem_path, time_range=None):
        cmd = 'SELECT Tick,Data,IsCompressed FROM CollectionRecords '
        if time_range is not None:
            cmd += 'WHERE '
            if type(time_range) in (int, float):
                time_range = (time_range, time_range)

            assert type(time_range) in (list,tuple) and len(time_range) == 2
            where_clauses = []
            if time_range[0] >= 0:
                # Find the first time value that is greater than or equal to 
                # the lower bound.
                start_time = None
                for i,time_val in enumerate(self._time_vals):
                    if time_val >= time_range[0]:
                        # Go back <heartbeat> number of ticks to get to the
                        # start of the data. The entire [t1-heartbeat:t2]
                        # range will be "replayed" to find the current data
                        # values. We have to go back <heartbeat> number of
                        # ticks to ensure that we encounter a "full" data
                        # dump -- followed by incremental dumps such as
                        # "carry-over" values (the data didn't change from
                        # the previous tick) etc.
                        start_idx = i - self._heartbeat
                        start_idx = max(0, start_idx)
                        start_time = self._time_vals[start_idx]
                        break

                where_clauses.append(' Tick>={} '.format(start_time))

            if time_range[1] >= 0:
                where_clauses.append(' Tick<={} '.format(time_range[1]))

            cmd += ' AND '.join(where_clauses)

        cmd += ' ORDER BY Tick ASC'
        self.cursor.execute(cmd)

        # We have to reset all the replayers even though all but one
        # of them are NOT for <elem_path>. This is because the replayers
        # are stateful and we have to reset them all to ensure that the
        # one we are interested in is in a clean state.
        for replayer in self._replayers_by_elem_path.values():
            replayer.Reset()

        requested_elem_path = elem_path
        for tick, data_blob, is_compressed in self.cursor.fetchall():
            if is_compressed:
                data_blob = zlib.decompress(data_blob)

            while True:
                # The first 2 bytes of any blob is a collectable ID,
                # followed by the raw bytes of that collectable, then
                # another collectable ID, and so on.
                cid = struct.unpack('H', data_blob[:2])[0]
                if self.IsDevDebug():
                    print ('[simdb verbose] tick {}, cid {}'.format(tick, cid))

                data_blob = data_blob[2:]

                replayer = self._replayers_by_elem_path[self.simhier.GetElemPath(cid)]
                is_auto_collected = cid in self._auto_collected_cids
                is_dev_debug = self.IsDevDebug()
                num_bytes_read = replayer.Replay(tick, data_blob, is_auto_collected, is_dev_debug)
                if num_bytes_read == 0:
                    break

                data_blob = data_blob[num_bytes_read:]
                if len(data_blob) == 0:
                    break

        time_vals = []
        data_vals = []

        ticks = list(self._replayers_by_elem_path[requested_elem_path].values_by_tick.keys())
        ticks.sort()

        deserializer = self.GetDeserializer(requested_elem_path)
        for tick in ticks:
            if time_range is None or (tick >= time_range[0] and tick <= time_range[1]):
                time_vals.append(tick)

                data_vals.append([])
                data_blobs = self._replayers_by_elem_path[requested_elem_path].values_by_tick[tick]
                for data_blob in data_blobs:
                    data_vals[-1].append(deserializer.Deserialize(data_blob))

        return {'TimeVals': time_vals, 'DataVals': data_vals}

    def GetAllTimeVals(self):
        return copy.deepcopy(self._time_vals)

class PODReplayer:
    NUM_BYTES_MAP = {
        'char': 1,
        'int8_t': 1,
        'int16_t': 2,
        'int32_t': 4,
        'int64_t': 8,
        'uint8_t': 1,
        'uint16_t': 2,
        'uint32_t': 4,
        'uint64_t': 8,
        'float_t': 4,
        'double_t': 8,
        'bool': 4
    }

    FORMAT_CODES_MAP = {
        'char': 'c',
        'int8_t':'b',
        'int16_t':'h',
        'int32_t':'i',
        'int64_t':'q',
        'uint8_t':'B',
        'uint16_t':'H',
        'uint32_t':'I',
        'uint64_t':'Q',
        'float_t':'f',
        'double_t':'d',
        'bool':'I'
    }

    class Actions(IntEnum):
        WRITE = 0
        CARRY = 1

    def __init__(self, dtype):
        self.num_bytes = PODReplayer.NUM_BYTES_MAP[dtype]
        self.format_code = PODReplayer.FORMAT_CODES_MAP[dtype]
        self.Reset()

    def Reset(self):
        self.values_by_tick = {}

    def Replay(self, tick, data_blob, is_auto_collected, is_dev_debug):
        # For collectables that are <16 bytes, we always write them directly
        # without the Action enum.
        if self.num_bytes < 16 or not is_auto_collected:
            val = struct.unpack(self.format_code, data_blob[:self.num_bytes])[0]
            self.values_by_tick[tick] = val
            if is_dev_debug:
                print ('[simdb verbose] {}<16, {}: {}'.format(self.num_bytes, GetDataTypeStr(self.format_code), val))
            return self.num_bytes

        # Actions are at the top of the blob as a uint8_t.
        action = self.Actions(struct.unpack('B', data_blob[:1])[0])

        if action == self.Actions.WRITE:
            if is_dev_debug:
                print ('[simdb verbose] WRITE {} bytes'.format(self.num_bytes))
            self.values_by_tick[tick] = struct.unpack(self.format_code, data_blob[1:1+self.num_bytes])[0]
            return 1 + self.num_bytes
        else:
            if is_dev_debug:
                print ('[simdb verbose] CARRY')
            self.values_by_tick[tick] = self.values_by_tick[self.__GetLastTick(tick)]
            return 1

    def __GetLastTick(self, tick):
        prev_ticks = set(self.values_by_tick.keys())
        for prev_tick in range(tick-1, -1, -1):
            if prev_tick in prev_ticks:
                return prev_tick

        return None

class PODDeserializer:
    NUM_BYTES_MAP = {
        'char': 1,
        'int8_t': 1,
        'int16_t': 2,
        'int32_t': 4,
        'int64_t': 8,
        'uint8_t': 1,
        'uint16_t': 2,
        'uint32_t': 4,
        'uint64_t': 8,
        'float_t': 4,
        'double_t': 8,
        'bool': 4
    }

    FORMAT_CODES_MAP = {
        'char': 'c',
        'int8_t':'b',
        'int16_t':'h',
        'int32_t':'i',
        'int64_t':'q',
        'uint8_t':'B',
        'uint16_t':'H',
        'uint32_t':'I',
        'uint64_t':'Q',
        'float_t':'f',
        'double_t':'d',
        'bool':'I'
    }

    def __init__(self, dtype):
        self.num_bytes = PODDeserializer.NUM_BYTES_MAP[dtype]
        self.format_code = PODDeserializer.FORMAT_CODES_MAP[dtype]

    def Deserialize(self, data_blob):
        return struct.unpack(self.format_code, data_blob[:self.num_bytes])[0]

class StringReplayer:
    class Actions(IntEnum):
        WRITE = 0
        CARRY = 1

    def __init__(self, strings_by_int):
        self.strings_by_int = strings_by_int
        self.Reset()

    def Reset(self):
        self.values_by_tick = {}

    def Replay(self, tick, data_blob, is_auto_collected, is_dev_debug):
        # TODO cnyce - fill this class out e.g. manually collected Collectable<std::string>,
        # CARRY, etc.
        string_id = struct.unpack('I', data_blob[:4])[0]
        self.values_by_tick[tick] = self.strings_by_int[string_id]
        return 4

class StringDeserializer:
    def __init__(self, strings_by_int):
        self.strings_by_int = strings_by_int
        self.format_code = 'I'

    def Deserialize(self, data_blob):
        string_id = struct.unpack('I', data_blob[:4])[0]
        return self.strings_by_int[string_id]

class EnumReplayer:
    class Actions(IntEnum):
        WRITE = 0
        CARRY = 1

    def __init__(self, dtype, enums_by_name):
        self.enum_handler = enums_by_name[dtype]
        self.num_bytes = self.enum_handler.GetNumBytes()
        self.format_code = self.enum_handler.GetFormatCode()
        self.Reset()

    def Reset(self):
        self.values_by_tick = {}

    def Replay(self, tick, data_blob, is_auto_collected, is_dev_debug):
        # For collectables that are <16 bytes, we always write them directly
        # without the Action enum.
        if self.num_bytes < 16 or not is_auto_collected:
            enum_int = struct.unpack(self.format_code, data_blob[:self.num_bytes])[0]
            self.values_by_tick[tick] = self.enum_handler.Convert(enum_int)
            if is_dev_debug:
                print ('[simdb verbose] {}<16, {}: {}'.format(self.num_bytes, GetDataTypeStr(self.format_code), enum_int))
            return self.num_bytes

        # Actions are at the top of the blob as a uint8_t.
        action = self.Actions(struct.unpack('B', data_blob[:1])[0])

        if action == self.Actions.WRITE:
            enum_int = struct.unpack(self.format_code, data_blob[1:1+self.num_bytes])[0]
            self.values_by_tick[tick] = self.enum_handler.Convert(enum_int)
            if is_dev_debug:
                print ('[simdb verbose] WRITE {} bytes'.format(self.num_bytes))
            return 1 + self.num_bytes
        else:
            self.values_by_tick[tick] = self.values_by_tick[self.__GetLastTick(tick)]
            if is_dev_debug:
                print ('[simdb verbose] CARRY')
            return 1

    def __GetLastTick(self, tick):
        prev_ticks = set(self.values_by_tick.keys())
        for prev_tick in range(tick-1, -1, -1):
            if prev_tick in prev_ticks:
                return prev_tick

        return None

class EnumDeserializer:
    def __init__(self, enum_handler):
        self.enum_handler = enum_handler
        self.format_code = self.enum_handler.GetFormatCode()

    def Deserialize(self, data_blob):
        num_bytes = self.enum_handler.GetNumBytes()
        enum_int = struct.unpack(self.format_code, data_blob[:num_bytes])[0]
        return self.enum_handler.Convert(enum_int)

class StructDeserializer:
    FORMAT_CODES_MAP = {
        'char_t':'c',
        'int8_t':'b',
        'int16_t':'h',
        'int32_t':'i',
        'int64_t':'q',
        'uint8_t':'B',
        'uint16_t':'H',
        'uint32_t':'I',
        'uint64_t':'Q',
        'float_t':'f',
        'double_t':'d',
    }

    NUM_BYTES_MAP = {
        'c':1,
        'b':1,
        'h':2,
        'i':4,
        'q':8,
        'B':1,
        'H':2,
        'I':4,
        'Q':8,
        'f':4,
        'd':8
    }

    def __init__(self, data_retriever, struct_name, strings_by_int, enums_by_name):
        self.data_retriever = data_retriever
        self.struct_name = struct_name
        self.strings_by_int = strings_by_int
        self.enums_by_name = enums_by_name
        self.field_deserializers = []
        self.field_format_codes = []
        self.all_field_names = []

    def AddField(self, field_name, field_deserializer, format_code):
        self.field_deserializers.append(field_deserializer)
        self.field_format_codes.append(format_code)
        self.all_field_names.append(field_name)

    def GetAllFieldNames(self):
        return copy.deepcopy(self.all_field_names)

    def GetVisibleFieldNames(self):
        visible_field_names = self.data_retriever.GetCurrentViewSettings().get(self.struct_name, {}).get('displayed_columns', [])
        return copy.deepcopy(visible_field_names)

    def Deserialize(self, data_blob):
        visible_field_names = self.data_retriever.GetCurrentViewSettings().get(self.struct_name, {}).get('displayed_columns', [])
        assert len(visible_field_names) > 0

        res = {}
        tiny_start = 0
        for i, deserializer in enumerate(self.field_deserializers):
            code = deserializer.format_code
            nbytes = self.NUM_BYTES_MAP[code]
            tiny_blob = data_blob[tiny_start:tiny_start+nbytes]
            tiny_start += nbytes

            field_name = self.all_field_names[i]
            if field_name not in visible_field_names:
                continue

            field_deserializer = self.field_deserializers[i]
            res[field_name] = field_deserializer.Deserialize(tiny_blob)

        return res

class ScalarStructReplayer:
    class Actions(IntEnum):
        WRITE = 0
        CARRY = 1

    def __init__(self, struct_name, struct_num_bytes):
        self.struct_name = struct_name
        self.struct_num_bytes = struct_num_bytes
        self.Reset()

    def Reset(self):
        self.values_by_tick = {}

    def Replay(self, tick, data_blob, is_auto_collected, is_dev_debug):
        # For collectables that are <16 bytes, we always write them directly
        # without the Action enum.
        if self.struct_num_bytes < 16 or not is_auto_collected:
            self.values_by_tick[tick] = data_blob[:self.struct_num_bytes]
            if is_dev_debug:
                print ('[simdb verbose] {}<16, {}: {}'.format(self.struct_num_bytes, self.struct_name, self.values_by_tick[tick]))
            return self.struct_num_bytes

        # Actions are at the top of the blob as a uint8_t.
        action = self.Actions(struct.unpack('B', data_blob[:1])[0])

        if action == self.Actions.WRITE:
            if is_dev_debug:
                print ('[simdb verbose] WRITE {} bytes'.format(self.struct_num_bytes))
            self.values_by_tick[tick] = data_blob[1:1+self.struct_num_bytes]
            return 1 + self.struct_num_bytes
        else:
            if is_dev_debug:
                print ('[simdb verbose] CARRY')
            self.values_by_tick[tick] = self.values_by_tick[self.__GetLastTick(tick)]
            return 1

    def __GetLastTick(self, tick):
        prev_ticks = set(self.values_by_tick.keys())
        for prev_tick in range(tick-1, -1, -1):
            if prev_tick in prev_ticks:
                return prev_tick

        return None

class ContigIterableReplayer:
    class Actions(IntEnum):
        ARRIVE = 0
        DEPART = 1
        BOOKENDS = 2
        CHANGE = 3
        CARRY = 4
        FULL = 5

    def __init__(self, struct_num_bytes, capacity):
        self.struct_num_bytes = struct_num_bytes
        self.capacity = capacity
        self.Reset()

        self.num_bytes_to_advance = {
            self.Actions.ARRIVE: 1 + self.struct_num_bytes,
            self.Actions.DEPART: 1,
            self.Actions.BOOKENDS: 1 + self.struct_num_bytes,
            self.Actions.CHANGE: 3 + self.struct_num_bytes,
            self.Actions.CARRY: 1
        }

    def Reset(self):
        self.values = None
        self.values_by_tick = {}

    def Replay(self, tick, data_blob, is_auto_collected, is_dev_debug):
        # TODO cnyce - manually collected containers
        assert is_auto_collected

        # Actions are at the top of the blob as a uint8_t.
        action = self.Actions(struct.unpack('B', data_blob[:1])[0])

        if action == self.Actions.FULL:
            # The number of structs is given here as a uint16_t
            size = struct.unpack('H', data_blob[1:3])[0]
            if is_dev_debug:
                print ('[simdb verbose] FULL with {} elements'.format(size))
            self.values = []
            for i in range(size):
                struct_blob = data_blob[3+i*self.struct_num_bytes:3+(i+1)*self.struct_num_bytes]
                self.values.append(struct_blob)

            self.values_by_tick[tick] = copy.deepcopy(self.values)
            return 3 + size*self.struct_num_bytes

        if action == self.Actions.ARRIVE:
            if is_dev_debug:
                print ('[simdb verbose] ARRIVE {} bytes'.format(self.struct_num_bytes))
            if self.values is not None:
                # Exactly one struct arrived and gets appended to the list (back).
                struct_blob = data_blob[1:1+self.struct_num_bytes]
                self.values.append(struct_blob)
                self.values_by_tick[tick] = copy.deepcopy(self.values)
            return self.num_bytes_to_advance[action]

        elif action == self.Actions.DEPART:
            if is_dev_debug:
                print ('[simdb verbose] DEPART')
            if self.values is not None:
                # Exactly one struct left and gets removed from the list (front).
                self.values.pop(0)
                self.values_by_tick[tick] = copy.deepcopy(self.values)
            return self.num_bytes_to_advance[action]

        elif action == self.Actions.CHANGE:
            # The changed bin index is written to the blob as a uint16_t, followed
            # by the changed struct's bytes.
            bin_idx = struct.unpack('H', data_blob[1:3])[0]
            if is_dev_debug:
                print ('[simdb verbose] CHANGE index {}, {} bytes'.format(bin_idx, self.struct_num_bytes))
            if self.values is not None:
                struct_blob = data_blob[3:3+self.struct_num_bytes]
                self.values[bin_idx] = struct_blob
                self.values_by_tick[tick] = copy.deepcopy(self.values)
            return self.num_bytes_to_advance[action]

        elif action == self.Actions.BOOKENDS:
            if is_dev_debug:
                print ('[simdb verbose] BOOKENDS, appended {} bytes'.format(self.struct_num_bytes))
            if self.values is not None:
                # This means that exactly one struct came arrived (append) and one struct
                # departed (pop front).
                struct_blob = data_blob[1:1+self.struct_num_bytes]
                self.values.append(struct_blob)
                self.values.pop(0)
                self.values_by_tick[tick] = copy.deepcopy(self.values)
            return self.num_bytes_to_advance[action]

        elif action == self.Actions.CARRY:
            if is_dev_debug:
                print ('[simdb verbose] CARRY')
            if self.values is not None:
                last_tick = self.__GetLastTick(tick)
                self.values_by_tick[tick] = copy.deepcopy(self.values_by_tick[last_tick])
            return self.num_bytes_to_advance[action]

        assert False, 'Unreachable'

    def __GetLastTick(self, tick):
        prev_ticks = set(self.values_by_tick.keys())
        for prev_tick in range(tick-1, -1, -1):
            if prev_tick in prev_ticks:
                return prev_tick

        return None

class SparseIterableReplayer:
    def __init__(self, struct_num_bytes, capacity):
        self.struct_num_bytes = struct_num_bytes
        self.capacity = capacity
        self.Reset()

    def Reset(self):
        self.values = [None]*self.capacity
        self.values_by_tick = {}

    def Replay(self, tick, data_blob, is_auto_collected, is_dev_debug):
        # TODO cnyce - manually collected containers
        assert is_auto_collected

        # The top of the blob always has the number of structs as a uint16_t.
        size = struct.unpack('H', data_blob[:2])[0]
        if is_dev_debug:
            print ('[simdb verbose] num valid: {}'.format(size))

        for i in range(size):
            # Each entry is preceeded by a uint16_t bin index.
            bin_idx = struct.unpack('H', data_blob[2+i*2:2+(i+1)*2])[0]
            struct_blob = data_blob[2+size*2+i*self.struct_num_bytes:2+size*2+(i+1)*self.struct_num_bytes]

            assert bin_idx >= 0 and bin_idx < self.capacity and bin_idx < len(self.values)
            self.values[bin_idx] = struct_blob

            if is_dev_debug:
                print ('[simdb verbose] bin {}, {} bytes'.format(bin_idx, self.struct_num_bytes))

        self.values_by_tick[tick] = copy.deepcopy(self.values)
        return 2 + size*2 + size*self.struct_num_bytes

class EnumDef:
    def __init__(self, name, int_type):
        self._name = name
        self.strings_by_int = {}

        if int_type == 'int8_t':
            self._format =  'b'
        elif int_type == 'int16_t':
            self._format =  'h'
        elif int_type == 'int32_t':
            self._format =  'i'
        elif int_type == 'int64_t':
            self._format =  'q'
        elif int_type == 'uint8_t':
            self._format =  'B'
        elif int_type == 'uint16_t':
            self._format =  'H'
        elif int_type == 'uint32_t':
            self._format =  'I'
        elif int_type == 'uint64_t':
            self._format =  'Q'
        else:
            raise ValueError('Invalid enum integer type: ' + int_type)

    def AddEntry(self, enum_string, enum_blob):
        int_val = struct.unpack(self._format, enum_blob)[0]
        self.strings_by_int[int_val] = enum_string

    def GetNumBytes(self):
        return struct.calcsize(self._format)

    def GetFormatCode(self):
        return self._format

    def Convert(self, val):
        return self.strings_by_int[val]

def GetDataTypeStr(format_code):
    if format_code == 'c':
        return 'char'
    elif format_code == 'b':
        return 'int8_t'
    elif format_code == 'h':
        return 'int16_t'
    elif format_code == 'i':
        return 'int32_t'
    elif format_code == 'q':
        return 'int64_t'
    elif format_code == 'B':
        return 'uint8_t'
    elif format_code == 'H':
        return 'uint16_t'
    elif format_code == 'I':
        return 'uint32_t'
    elif format_code == 'Q':
        return 'uint64_t'
    elif format_code == 'f':
        return 'float_t'
    elif format_code == 'd':
        return 'double_t'
    else:
        raise ValueError('Invalid format code: ' + format_code)
