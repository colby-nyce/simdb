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
from viewer.model.data_deserializers import ReadOffBytes
def ValidateCollectionAtTime(timestamp_id, time_point):
    cursor.execute(f'SELECT Records FROM CollectionRecords WHERE TimestampID={timestamp_id}')
    rows = cursor.fetchall()
    assert len(rows) == 1
    data_bytes = rows[0]

    while data_bytes:
        # First 2 bytes are always the CID (uint16_t)
        (cid, data_bytes) = ReadOffBytes('H', data_bytes)

        # Get the deserializer for the CID
        deserializer = deserializers_by_cid[cid]

        # Ask it to deserialize the bytes
        (ours, data_bytes) = deserializer.Deserialize(data_bytes)

        # If we deserialized something that is not an int, string, or bool,
        # ask it to convert itself to a dict so we can compare against JSON.
        if not isinstance(ours, int  ) and \
           not isinstance(ours, float) and \
           not isinstance(ours, str  ) and \
           not isinstance(ours, bool ):
           ours = ours.toDict()

        # Compare against JSON
        truth = expected_data[str(time_point)][str(cid)]
        assert ours == truth

        # Remove this CID from the expected data
        del expected_data[str(time_point)][str(cid)]

    # Assert that we consumed everything we were expecting to find
    assert len(expected_data[str(time_point)])
    del expected_data[str(time_point)]

# Run validation for all collection timestamps
cursor.execute('SELECT Id,Timestamp FROM Timestamps')
for timestamp_id, time_point in cursor.fetchall():
    ValidateCollectionAtTime(timestamp_id, time_point)

# Assert that we consumed everything we were expecting to find
assert len(expected_data) == 0
