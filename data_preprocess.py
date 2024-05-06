from collections import Counter
import csv
import sys

raw_data_file = sys.argv[1]
record_sep_file = sys.argv[2]
label_append = sys.argv[3]

with open(raw_data_file, "r") as file:
    reader = csv.reader(file)
    rows = list(reader)

# No. | Time | Source | Destination | Protocol | Length | Info
data = []
data.append(rows[0])
del rows[0]

with open(record_sep_file, "r") as file:
    record_seps = [no.strip() for no in file.readlines()]

# For each record:
# - Packet Count
# - Total Length
# - Average Packet Interval
# - Max Packet Interval
# - Min Packet Interval
# - Average Packet Length
# - Max Packet Length
# - Min Packet Length
# - Most Common Packet Length
record = []
records = []
for row in rows:
    record.append(row)
    if row[0] in record_seps:
        data = []
        # Calculate packet count and total length
        packet_count = len(record)
        packet_lengths = [int(pkt[-2]) for pkt in record]
        total_length = sum(packet_lengths)
        data.extend([packet_count, total_length])

        # Calculate packet intervals
        intervals = []
        for i in range(1, len(record)):
            intervals.append(float(record[i][1]) - float(record[i - 1][1]))
        avg_pkt_interval = sum(intervals) / len(intervals)
        max_pkt_interval, min_pkt_interval = max(intervals), min(intervals)
        data.extend([avg_pkt_interval, max_pkt_interval, min_pkt_interval])

        # Calculate packet lengths
        avg_pkt_length = total_length / packet_count
        max_pkt_length, min_pkt_length = max(packet_lengths), min(packet_lengths)
        c = Counter(packet_lengths)
        most_common_pkt_length = c.most_common(1)[0][0]
        data.extend([avg_pkt_length, max_pkt_length, min_pkt_length, most_common_pkt_length])

        data.append(label_append)
        records.append(data)
        record = []

with open(raw_data_file.rstrip("raw.csv") + "train.csv", "w") as file:
    writer = csv.writer(file, delimiter=',')
    writer.writerow(["Packet_Count", "Total_Length", "Avg_Pkt_Interval", "Max_Pkt_Interval", "Min_Pkt_Interval", "Avg_Pkt_Length", "Max_Pkt_Length", "Min_Pkt_Length", "Most_Common_Pkt_Length", "Label"])
    writer.writerows(records)