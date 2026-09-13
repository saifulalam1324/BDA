import xml.etree.ElementTree as ET
import csv
import os
import time
XML_FILE = r"C:\BDA\Comments.xml"
CSV_FILE = r"C:\BDA\Comments.csv"
start = time.time()
print("Pass 1/2: Scanning XML for columns...")
fields = set()
count = 0
for event, elem in ET.iterparse(XML_FILE, events=("end",)):
    if elem.tag == "row":
        fields.update(elem.attrib.keys())
        count += 1
        if count % 100000 == 0:
            print(f"  Scanned {count:,} rows...")
        elem.clear()
fields = sorted(fields)
print(f"Found {len(fields)} columns")
print(f"Found {count:,} rows")
print("\nPass 2/2: Converting to CSV...")
count = 0
with open(
    CSV_FILE,
    "w",
    newline="",
    encoding="utf-8",
    buffering=1024 * 1024
) as f:
    writer = csv.DictWriter(
        f,
        fieldnames=fields,
        extrasaction="ignore"
    )
    writer.writeheader()
    for event, elem in ET.iterparse(XML_FILE, events=("end",)):
        if elem.tag == "row":
            writer.writerow(elem.attrib)
            count += 1
            if count % 100000 == 0:
                print(f"  Converted {count:,} rows...")
            elem.clear()
elapsed = time.time() - start
size_mb = os.path.getsize(CSV_FILE) / (1024 * 1024)
