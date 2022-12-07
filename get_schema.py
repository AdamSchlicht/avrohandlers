import avro
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

filename = input("Avro file name: ")
reader = DataFileReader(open(filename,"rb"), DatumReader())
schema = reader.meta

schema = schema['avro.schema'].decode("utf-8")

schema_file = open("schema.avsc", "w")
n = schema_file.write(schema)
schema_file.close()