#!/usr/bin/env python3


import sys
import argparse
import os
import os.path
import json

filename = sys.argv[1]
libname = "db"
if len(sys.argv) >= 3:
    libname = sys.argv[2]

os.makedirs(libname, exist_ok=True)
outfilename = "%s/__init__.py" % libname
python_block_indent = "    "
db_uri = 'sqlite:////tmp/database.sql'



lines = []
with open(filename, "r") as f:
    lines = [ l.strip() for l in f.read().split("\n") if len(l.strip()) > 0 ]



entities = {}

# get entity names
for line in lines:
    if line.find("object ") == 0:
        entity_name = line.split()[1]
        entities[entity_name] = {}

# get members
for line in lines:
    if line.find(":") > -1 and line.split(":")[0].find("-") < 0:
        if line.split()[0] in entities:
            entity_name = line.split()[0]
            col = line.split(":")[1].split("(")[0].strip()
            coltype = line.split("(")[1].split(")")[0].strip()
            #print("object(%s) member(%s) type(%s)" % (entity_name, col, coltype))
            entities[entity_name][col] = coltype
relationships = {}

# get foreign key constraints
for line in lines:
    if line.find(" <- ") > -1:
        line_tmp = line.replace(" ", "")
        line_tmp = line_tmp.replace("<-", ":")
        line_tmp = line_tmp.replace('"', "")
        (fk_targettable, fk_sourcetable, fk_member) = line_tmp.split(":")
        if entities[fk_sourcetable][fk_member] == "FK":
            print("Found valid FK")
            entities[fk_sourcetable][fk_member] = "Integer, ForeignKey('%s.%s_id')" % (fk_targettable, fk_targettable)
            if fk_sourcetable not in relationships:
                relationships[fk_sourcetable] = {}
            if fk_targettable not in relationships:
                relationships[fk_targettable] = {}
            relationships[fk_sourcetable][fk_targettable] = "%s = relationship('%s', back_populates='%ss')" % (fk_targettable, fk_targettable, fk_sourcetable)
            relationships[fk_targettable][fk_sourcetable] = "%ss = relationship('%s', back_populates='%s')" % (fk_sourcetable, fk_sourcetable, fk_targettable)
        else:
            print("ERROR (%s.%s refs %s)" % (fk_sourcetable, fk_member, fk_targettable))


outfile = open(outfilename, "w")
outfile.write("#!/usr/bin/env python3\n")
outfile.write("\n")
outfile.write("\n")
outfile.write("from sqlalchemy.ext.declarative import declarative_base\n")
outfile.write("from sqlalchemy import Column, Integer, String, Numeric, DateTime, Date\n")
outfile.write("from sqlalchemy import ForeignKey\n")
outfile.write("from sqlalchemy.sql import func\n")
outfile.write("from sqlalchemy.orm import relationship\n")
outfile.write("\n")
outfile.write("Base = declarative_base()\n")

import re
digits_only = re.compile(r'[^\d]+')

for entity_name in entities:
    outfile.write("\n")
    outfile.write("class %s(Base):\n" % entity_name)
    outfile.write("%s__tablename__ = '%s'\n" % (python_block_indent, entity_name))
    for col in entities[entity_name]:
        coltype = entities[entity_name][col]
        if coltype == "PK":
            coltype = "Integer, primary_key=True"
        elif coltype == "INT":
            coltype = "Integer"
        elif coltype == "DATETIME":
            coltype = "DateTime"
        elif coltype.find("TEXT") == 0:
            length = 256
            length_tmp = digits_only.sub('', coltype)
            if len(length_tmp) > 0:
                length = int(length_tmp)
            coltype = "String(%d)" % length
        elif coltype.find("Integer,") == 0:
            pass
        elif coltype == "FK":
            print("ERROR unresolved FK (%s.%s)" % (entity_name, col))
        else:
            print("ERROR coltype unknown (%s)" % coltype)
        outfile.write("%s%s = Column(%s)\n" % (python_block_indent, col, coltype))

    if entity_name in relationships:
        # we have foreign keys, let's build relationships
        for fk_targettable in relationships[entity_name]:
            outfile.write("%s%s\n" % (python_block_indent, relationships[entity_name][fk_targettable]))

    outfile.write("\n")
    outfile.write("%sdef to_dict(self):\n" % (python_block_indent*1))
    outfile.write("%sreturn {c.name: getattr(self, c.name) for c in self.__table__.columns}\n" % (python_block_indent*2))
    outfile.write("\n")
    outfile.write("def %s_from_dict(jsonString):\n" % (entity_name))
    outfile.write("%simport json\n" % (python_block_indent*1))
    outfile.write("%so = json.loads(jsonString)\n" % (python_block_indent*1))
    param_init = []
    for col in entities[entity_name]:
        outfile.write("%sif %s not in o.keys():\n" % (python_block_indent*1, col))
        outfile.write("%so['%s'] = None\n" % (python_block_indent*2, col))
    for col in entities[entity_name]:
        param_init.append("%s=o['%s']" % (col, col))
    
    outfile.write("%sreturn %s(%s)\n" % (python_block_indent*1, entity_name, ", ".join(param_init)))
    outfile.write("\n")

outfile.write("\n")
outfile.write("\n")
outfile.write("from sqlalchemy import create_engine\n")
outfile.write("from sqlalchemy.orm import sessionmaker\n")
outfile.write("\n")
outfile.write("engine = None\n")
outfile.write("Session = None\n")
outfile.write("session = None\n")
outfile.write("uri_ = None\n")
outfile.write("\n")
outfile.write("def conf_uri(uri=None):\n")
outfile.write("%s\n" % python_block_indent)
outfile.write("%sglobal uri_\n" % python_block_indent)
outfile.write("%suri_ = uri\n" % python_block_indent)
outfile.write("\n")
outfile.write("def load():\n")
outfile.write("%s\n" % python_block_indent)
outfile.write("%sglobal engine\n" % python_block_indent)
outfile.write("%sglobal Session\n" % python_block_indent)
outfile.write("%sglobal session\n" % python_block_indent)
outfile.write("%sglobal Base\n" % python_block_indent)
outfile.write("%s\n" % python_block_indent)
outfile.write("%sif uri_ == None:\n" % python_block_indent)
outfile.write("%sengine = create_engine('sqlite:///:memory:', echo=True)\n" % (python_block_indent*2))
outfile.write("%selse:\n" % python_block_indent)
outfile.write("%sengine = create_engine(uri_)\n" % (python_block_indent*2))
outfile.write("%sSession = sessionmaker(bind=engine)\n" % python_block_indent)
outfile.write("%ssession = Session()\n" % python_block_indent)
outfile.write("\n")
outfile.write("def zeroize():\n")

outfile.write('%sif uri_.find("sqlite:///") == 0:\n' % (python_block_indent))
outfile.write('%sfilename = uri_[len("sqlite:///"):]\n' % (python_block_indent*2))
outfile.write('%simport os\n' % (python_block_indent*2))
outfile.write('%sos.remove(filename)\n' % (python_block_indent*2))
outfile.write('%sBase.metadata.create_all(engine)\n' % (python_block_indent*2))

outfile.write('%sif uri_.find("mysql:") == 0:\n' % python_block_indent)
outfile.write('%sengine.execute("set FOREIGN_KEY_CHECKS=0;")\n' % (python_block_indent*2))
outfile.write('%sBase.metadata.drop_all(engine)\n' % (python_block_indent*2))
outfile.write('%sengine.execute("set FOREIGN_KEY_CHECKS=1;")\n' % (python_block_indent*2))
outfile.write('%sBase.metadata.create_all(engine)\n' % (python_block_indent*2))

outfile.write('%sif uri_.find("postgresql+pg8000:") == 0:\n' % python_block_indent)
outfile.write('%sBase.metadata.drop_all(engine)\n' % (python_block_indent*2))
outfile.write('%sBase.metadata.create_all(engine)\n' % (python_block_indent*2))

outfile.write("\n")

outfile.write("# copy pase import code\n")
for e in entities:
    outfile.write("#from %s import %s\n" % (libname, e))

outfile.write("\n")


outfile.flush()
outfile.close()
