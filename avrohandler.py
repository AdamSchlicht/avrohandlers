import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader


schema = avro.schema.parse(open("WPT_WRAP_SALES.avsc", "rb").read())

#writer = DataFileWriter(open("wtp_wrap_sales_small.avro", "wb"), DatumWriter(), schema)
# writer.append({"name": "Alyssa", "favorite_number": 256})
# writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
# writer.close()

reader = DataFileReader(open("wtp_wrap_sales.avro", "rb"), DatumReader())
count=0

for row in reader:
    # print(row)
    #writer.append(row)
    count+=1
    # print(count)

reader.close()
print(count)