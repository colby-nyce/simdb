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
parser = argparse.ArgumentParser(description='Script to validate collection data against JSON')
parser.add_argument('--db-file', help='Full or relative path to the database file', required=True)
parser.add_argument('--json-file', help='JSON file containing the expected data', required=True)
args = parser.parse_args()

db_file = args.db_file
assert(os.path.exists(db_file))

json_file = args.json_file
assert(os.path.exists(json_file))

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

# Expected data
import json
with open(json_file, 'r') as fin:
    expected_data = json.load(fin)

# Validate collected data bytes against the deserialized objects
from viewer.model.data_deserializers import ByteBuffer
def ValidateCollectionAtTime(timestamp_id, time_point):
    cursor.execute(f'SELECT Records FROM CollectionRecords WHERE TimestampID={timestamp_id}')
    rows = cursor.fetchall()[0]
    assert len(rows) == 1

    import zlib
    buf = ByteBuffer(zlib.decompress(rows[0]))

    while not buf.Done():
        # First 2 bytes are always the CID (uint16_t)
        cid = buf.Read('uint16_t')

        # Get the deserializer for the CID
        deserializer = deserializers_by_cid[cid]

        # Ask it to deserialize the bytes
        ours = deserializer.Deserialize(buf)

        # Compare against JSON
        truth = expected_data[str(time_point)][str(cid)]
        assert ours == truth

        # Remove this CID from the expected data
        del expected_data[str(time_point)][str(cid)]

    # Assert that we consumed everything we were expecting to find
    assert len(expected_data[str(time_point)]) == 0
    del expected_data[str(time_point)]

# Run validation for all collection timestamps
cursor.execute('SELECT Id,Timestamp FROM Timestamps')
for timestamp_id, time_point in cursor.fetchall():
    # Handle uint64_t (stored as strings)
    if isinstance(time_point, str):
        time_point = int(time_point)

    ValidateCollectionAtTime(timestamp_id, time_point)

# Assert that we consumed everything we were expecting to find
assert len(expected_data) == 0
