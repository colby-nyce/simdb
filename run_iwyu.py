import os, sys, json

with open('compile_commands.json', 'r') as fin:
    cmds_json = json.load(fin)

for cmd_json in cmds_json:
    cmd = cmd_json['command']
    cmd_parts = cmd.split(' ')
    aug_parts = []
    i = 0
    while i < len(cmd_parts)-1:
        if cmd_parts[i] == '-o' and cmd_parts[i+1].find('CMakeFiles') == 0:
            i += 2
        else:
            aug_parts.append(cmd_parts[i])
            i += 1

    aug_parts.append(cmd_parts[-1])
    print('Running command: ' + ' '.join(aug_parts))
    sys.stdout.flush()
    os.system(' '.join(aug_parts))
