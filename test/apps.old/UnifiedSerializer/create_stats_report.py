import argparse, json, sqlite3, struct, zlib

parser = argparse.ArgumentParser(description='Stats Report Exporter')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--timeseries', action='store_true', help='Export timeseries data')
group.add_argument(
    '--summary-stats',
    type=str,
    nargs='+',
    choices=['min', 'max', 'mean', 'final'],
    help='List of summary statistics to export (min/max/mean/final)'
)

parser.add_argument('--outfile', type=str, required=True, help='Output file for the report')
parser.add_argument('--time-range', type=int, nargs=2, help='Time range for the report (start end)')
parser.add_argument('--decimal-precision', type=int, default=3, help='Decimal precision for floating point values')
parser.add_argument("database", type=str, help="Path to the SimDB database file.")
args = parser.parse_args()

assert not args.summary_stats, "Not implemented yet"
time_range = args.time_range

db_conn = sqlite3.connect(args.database)
cursor = db_conn.cursor()

# TODO cnyce: We need a better way to differentiate between the different apps.
# This will break when we have multiple instances of the same StatsCollector app.
cmd = "SELECT Id FROM RegisteredApps WHERE AppName = 'StatsCollector'"
cursor.execute(cmd)
app_id = cursor.fetchone()[0]

cmd = f"SELECT StatName FROM StatsCollectorStatNames WHERE AppID = {app_id}"
cursor.execute(cmd)
stat_names = [row[0] for row in cursor.fetchall()]

# TODO cnyce: Make use of the UnifiedCollectorByteLayouts (YAML defns).
byte_format = 'd'*len(stat_names)

cmd = f"SELECT DataBlob, IsCompressed FROM UnifiedCollectorBlobs WHERE AppID = {app_id}"
if time_range:
    cmd += f" AND Tick >= {time_range[0]} AND Tick <= {time_range[1]}"

cursor.execute(cmd)
rows = cursor.fetchall()

csv_rows = []
csv_rows.append(stat_names)
for data_blob, is_compressed in rows:
    if is_compressed:
        data_blob = zlib.decompress(data_blob)
    data_values = struct.unpack(byte_format, data_blob)

    if args.decimal_precision is not None:
        data_values = [round(value, args.decimal_precision) for value in data_values]

    csv_rows.append(data_values)

with open(args.outfile, 'w') as outfile:
    for row in csv_rows:
        outfile.write(','.join(map(str, row)) + '\n')

print (f"Report written to {args.outfile}")
