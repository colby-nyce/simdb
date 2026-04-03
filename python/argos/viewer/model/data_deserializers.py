# Argos collection-record blob decoding.
from collections import OrderedDict
from viewer.model.dtype_inspector import DataTypeInspector

UNPACK_FORMATS = {
    'char':     'b',
    'int8_t':   'b',
    'uint8_t':  'B',
    'int16_t':  'h',
    'uint16_t': 'H',
    'int32_t':  'i',
    'uint32_t': 'I',
    'int64_t':  'q',
    'uint64_t': 'Q',
    'double':   'd',
    'float':    'f',
    'bool':     'B'
}

# Helper to take the first N bytes off a byte stream, convert
# according to the given format, and return the unpacked data
# along with the updated byte stream.
import struct
def ReadOffBytes(fmt, data_bytes):
    if fmt in UNPACK_FORMATS:
        fmt = UNPACK_FORMATS[fmt]
    extracted = struct.unpack(fmt, data_bytes)
    if len(fmt) == 1:
        extracted = extracted[0]
    data_bytes = data_bytes[struct.calcsize(fmt):]
    return (extracted, data_bytes)

def CreateDeserializer(inspector, dtype_name, tiny_strings=None):
    # Extract sparse/contig and capacity from a data type name
    def GetContainerMeta(dtype_name):
        def GetMeta(which):
            key = f'_{which}_capacity'
            idx = dtype_name.find(key)
            if idx != -1:
                base_dtype_name = dtype_name[:idx]
                capacity = int(dtype_name[len(key):])
                return (base_dtype_name, capacity)

            return None

        meta = GetMeta('sparse')
        if meta:
            return (meta[0], meta[1], 'sparse')
        if not meta:
            meta = GetMeta('contig')
            if meta:
                return (meta[0], meta[1], 'sparse')

        return None

    # Simple types (non-enum)
    if dtype_name in ('char', 'int8_t', 'uint8_t', 'int16_t', 'uint16_t', \
                      'int32_t', 'uint32_t', 'int64_t', 'uint64_t', \
                      'double', 'float', 'bool'):
        return SimpleDeserializer(dtype_name)

    # String types (collected as uint32_t)
    if dtype_name == 'std::string':
        return StringDeserializer(tiny_strings)

    # Enum types
    enum_map = inspector.GetEnumMap(dtype_name)
    if enum_map:
        backing_kind = inspector.GetEnumBackingKind(dtype_name)
        val_deserializer = SimpleDeserializer(backing_kind)
        return EnumDeserializer(val_deserializer, enum_map)

    # Container types
    container_meta = GetContainerMeta(dtype_name)
    if container_meta:
        dtype_name, capacity, sparse_mode = container_meta
        bin_deserializer = inspector.GetSerializer(dtype_name)
        if sparse_mode == 'sparse':
            return SparseContainerDeserializer(bin_deserializer, capacity)
        else:
            return ContigContainerDeserializer(bin_deserializer, capacity)

    # Struct types
    struct_defn = inspector.GetStructDefn(dtype_name)
    if struct_defn is None:
        raise Exception(f'Unable to create deserializer for data type: {dtype_name}')

    return StructDeserializer(struct_defn, inspector, tiny_strings)

# This class deserializes non-enum POD types.
class SimpleDeserializer:
    CONVERTERS = {
        'char':     lambda(x): return str(x),
        'int8_t':   lambda(x): return int(x),
        'uint8_t':  lambda(x): return int(x),
        'int16_t':  lambda(x): return int(x),
        'uint16_t': lambda(x): return int(x),
        'int32_t':  lambda(x): return int(x),
        'uint32_t': lambda(x): return int(x),
        'int64_t':  lambda(x): return int(x),
        'uint64_t': lambda(x): return int(x),
        'double':   lambda(x): return float(x),
        'float':    lambda(x): return float(x),
        'bool':     lambda(x): return bool(x)
    }

    def __init__(self, dtype_name):
        self._fmt = UNPACK_FORMATS[dtype_name]
        self._converter = self.CONVERTERS[dtype_name]

    def Deserialize(self, data_bytes):
        (val, data_bytes) = ReadOffBytes(self._fmt, data_bytes)
        return (self._converter(val), data_bytes)

# This class deserializes string types.
class StringDeserializer:
    def __init__(self, tiny_strings):
        assert tiny_strings is not None
        self._tiny_strings = tiny_strings

    def Deserialize(self, data_bytes):
        (string_id, data_bytes) = ReadOffBytes('uint32_t', data_bytes)
        return self._tiny_strings.GetString(string_id, must_exist=True)

# This class deserializes enum types.
class EnumDeserializer:
    def __init__(self, val_deserializer, enum_map):
        self._val_deserializer = val_deserializer
        self._enum_map = enum_map

    def Deserialize(self, data_bytes):
        (enum_val, data_bytes) = self._val_deserializer.Deserialize(data_bytes)
        return self._enum_map[(int)enum_val]

# This class deserializes contiguous container types.
class ContigContainerDeserializer:
    def __init__(self, bin_deserializer, capacity):
        self._bin_deserializer = bin_deserializer
        self._capacity = capacity

    def Deserialize(self, data_bytes):
        # First 2 bytes always give the container size
        (size, data_bytes) = ReadOffBytes('uint16_t', data_bytes)
        assert size <= self._capacity

        # Create the container
        container = []
        while len(container) < size:
            (bin_val, data_bytes) = self._bin_deserializer.Deserialize(data_bytes)
            container.append(bin_val)

        return container

# This class deserializes sparse container types.
class SparseContainerDeserializer:
    def __init__(self, bin_deserializer, capacity):
        self._bin_deserializer = bin_deserializer
        self._capacity = capacity

    def Deserialize(self, *args):
        # First 2 bytes always give the container size
        (size, data_bytes) = ReadOffBytes('uint16_t', data_bytes)
        assert size <= self._capacity

        # Create the container; recall that sparse containers
        # store None wherever there is no data
        container = [None] * self._capacity

        # Read each element (bin), noting that each one is
        # preceeded by a uint16_t which gives the bin idx.
        while size > 0:
            (bin_idx, data_bytes) = ReadOffBytes('uint16_t', data_bytes)
            (bin_val, data_bytes) = self._bin_deserializer.Deserialize(data_bytes)
            container[bin_idx] = bin_val
            size -= 1

        return container

# This class deserializes struct types.
class StructDeserializer:
    def __init__(self, struct_defn, inspector, tiny_strings):
        self._field_deserializers = OrderedDict()
        for field in struct_defn.children:
            if field.kind == 'pod':
                self._field_deserializers[field.name] = SimpleDeserializer(field.type_name)
            elif field.kind == 'string':
                self._field_deserializers[field.name] = StringDeserializer(tiny_strings)
            elif field.kind == 'enum':
                self._field_deserializers[field.name] = CreateDeserializer(inspector, field.type_name)
            elif field.kind == 'struct':
                self._field_deserializers[field.name] = StructDeserializer(field, inspector, tiny_strings)
            else:
                raise ValueError(f'Unknown field data type: {field.kind}')

    def Deserialize(self, data_bytes):
        deserialized = OrderedDict()
        for field_name, deserializer in self._field_deserializers.items():
            (field_val, data_bytes) = deserializer.Deserialize(data_bytes)
            deserialized[field_name] = field_val
        return deserialized
