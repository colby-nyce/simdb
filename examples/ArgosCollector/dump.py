import os, sys
from pathlib import Path

# Repo root is two levels above this directory.
_REPO_ROOT = Path(__file__).resolve().parents[2]

# Update the path if needed.
_ARGOS_PKG = _REPO_ROOT / 'python' / 'argos'
if str(_ARGOS_PKG) not in sys.path:
    sys.path.insert(0, str(_ARGOS_PKG))

# Arguments
import argparse
parser = argparse.ArgumentParser('Collection dumper')
parser.add_argument('--db-file', help='Full/relative path to the database file')
parser.add_argument('--quiet', help='Do not print deserialized data to stdout', action='store_true')
args = parser.parse_args()

db_file = args.db_file
assert os.path.exists(db_file)

# Access everything about collected data types
from viewer.model.dtype_inspector import DataTypeInspector
dtype_inspector = DataTypeInspector(db_file)

# Get a map of all collectable IDs to their deserializers
import sqlite3
conn = sqlite3.connect(db_file)
cursor = conn.cursor()
cursor.execute('SELECT TypeName,SerializationCID FROM CollectableTreeNodes')

deserializers_by_cid = {}
for type_name, cid in cursor.fetchall():
    deserializer = dtype_inspector.GetDeserializer(type_name)
    deserializers_by_cid[cid] = deserializer

# Dump the deserialized collectable values to stdout
def DumpCollectionAtTime(timestamp_id, time_point):
    cursor.execute(f'SELECT Records FROM CollectionRecords WHERE TimestampID={timestamp_id}')
    rows = cursor.fetchall()[0]
    assert len(rows) == 1

    import zlib
    from viewer.model.data_deserializers import ByteBuffer
    buf = ByteBuffer(zlib.decompress(rows[0]))

    if not args.quiet:
        print(f'At time point {time_point} we collected:')

    while not buf.Done():
        # First 2 bytes are always the CID (uint16_t)
        cid = buf.Read('H')

        # Get the deserializer for the CID
        deserializer = deserializers_by_cid[cid]

        # Ask it to deserialize the bytes
        val = deserializer.Deserialize(buf)

        # Dump
        if not args.quiet:
            print(f'CID {cid}: {val}')

# Dump collection at every time point
if not args.quiet:
    print('All collectables found in database:\n')

cursor.execute('SELECT Id,Timestamp FROM Timestamps')
for timestamp_id, time_point in cursor.fetchall():
    # Handle uint64_t (stored as strings)
    if isinstance(time_point, str):
        time_point = int(time_point)

    DumpCollectionAtTime(timestamp_id, time_point)
