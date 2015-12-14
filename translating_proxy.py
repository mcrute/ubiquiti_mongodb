#!/usr/bin/env python

import struct
from bson import BSON
from StringIO import StringIO


DB_OFFSET = 16 + 4 # headers + flags

OP_REPLY = 1
OP_UPDATE = 2001
OP_INSERT = 2002
OP_QUERY = 2004
OP_GET_MORE = 2005
OP_DELETE = 2006

# Touchup DB
OP_TOUCHUP_DB = set((OP_UPDATE, OP_INSERT, OP_QUERY, OP_GET_MORE, OP_DELETE))

def fixup_ns(raw_data, replace_with):
    try:
        data = BSON(raw_data).decode()
    except:
        return

    if "ns" in data:
        db, collection = data["ns"].split(".", 1)
        data["ns"] = ".".join((replace_with, collection))
        return BSON.encode(data)
    else:
        return raw_data


def fixup_db(data, replace_with):
    null = data.index("\x00")
    db, collection = data[:null].split(".", 1)
    replace_with = "admin" if db == "admin" else replace_with
    fixed_up = "{}.{}\x00".format(replace_with, collection)
    return fixed_up, (collection == "system.indexes")


def handle_line(line):
    output = StringIO()

    length, req_id, resp_to, msg_type, flags = struct.unpack_from("<iiiii", line)

    print msg_type

    if msg_type not in OP_TOUCHUP_DB:
        return

    db_collection, rewrite_body = fixup_db(line[DB_OFFSET:], "mfi")

    output.write(struct.pack("<iiii", req_id, resp_to, msg_type, flags))
    output.write(db_collection)

    if rewrite_body:
        body_start = DB_OFFSET + len(db_collection) + 8
        print repr(line[body_start:])
        print BSON(line[body_start:]).decode()
        output.write(fixup_ns(line[body_start:], "mfi"))
    else:
        body_start = DB_OFFSET + len(db_collection)
        body = struct.pack("<i", 2280) + line[body_start+4:]
        print struct.unpack_from("<i", line), len(body)
        print repr(body)
        print BSON(body).decode()
        output.write(line[body_start:])

    value = output.getvalue()
#    print repr(struct.pack("<i", len(value)) + value)




line = ''

handle_line(line)

#for line in open("unifi_snapshot.log"):
#    handle_line(eval(line))
