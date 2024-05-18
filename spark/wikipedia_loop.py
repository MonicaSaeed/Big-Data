from pyspark import SparkConf, SparkContext
from itertools import combinations
import re
import time

start_time = time.time()

# Create a SparkContext
conf = SparkConf().setMaster("local").setAppName("PopularWikipediaPagesLoop")
sc2 = SparkContext(conf=conf)

# Read the data from the file
with open("pagecounts-20160101-000000_parsed.out", "r", encoding="utf-8") as file:
    lines = file.readlines()

# Initialize variables for computing min, max, and average page sizes
total_size = 0
min_size = float('inf')
max_size = 0
total_count = 0

# Initialize dictionaries for title counts and combined titles
english_the_titles_count = 0
unique_terms_count = 0
unique_terms = {}
title_counts = {}
combined_titles = {}
titles_to_delete = []


# Loop over each line of data
for line in lines:
    fields = line.split()
    
    if len(fields) < 4:
        continue  
    
    project = fields[0]
    title = fields[1]    
    try:
        hits = int(fields[2])
    except ValueError:
        continue  
    try:
        size = int(fields[3])
    except ValueError:
        continue  


    # Update min and max page sizes
    min_size = min(min_size, size)
    max_size = max(max_size, size)
    total_size += size
    total_count += 1


    # Count English titles that start with "The" and not project "en"
    if project != "en" and title.startswith("The"):
        english_the_titles_count += 1


    # Preprocess title
    t = title.lower()
    t = re.sub(r'[^a-z0-9_]', '', t)
    # Count unique terms
    terms = t.split("_")
    for term in terms:
        if term not in unique_terms:
            unique_terms[term] = 1
            unique_terms_count += 1
        else:
            unique_terms[term] += 1


    #Extract each title and the number of times it was repeated
    if title in title_counts:
        title_counts[title] += 1
    else:
        title_counts[title] = 1


    # Combine titles
    if title in combined_titles:
        combined_titles[title].append((project, hits, size))
    else:
        combined_titles[title] = [(project, hits, size)]

# Iterate over combined_titles to identify titles with only one associated data list
for title, data_list in combined_titles.items():
    if len(data_list) == 1:
        titles_to_delete.append(title)

# Remove titles with only one associated data list
for title in titles_to_delete:
    del combined_titles[title]

# all_pairs = []
# # Iterate over combined_titles to generate pairs of pages with the same title
# for title, data_list in combined_titles.items():
#     if len(data_list) > 1:
#         # Generate all pairwise combinations
#         page_combinations = combinations(data_list, 2)
#         # Append each pair to the list
#         for pair in page_combinations:
#             all_pairs.append((title, pair[0], pair[1]))
# # Print or process first 15 pairs title -> (project, hits, size), (project, hits, size)
# for pair in all_pairs[:15]:
#     print("Title: {}, Page 1: {}, Page 2: {}".format(pair[0], pair[1], pair[2]))


# Compute average page size
avg_size = total_size / total_count
avg_size = round(avg_size, 5)

# Write results to file
with open("loop-results.txt", "w", encoding="utf-8") as f:
    f.write("Min page size: {}\n".format(min_size))
    f.write("Max page size: {}\n".format(max_size))
    f.write("Average page size: {}\n".format(avg_size))
    f.write("English The titles count: {}\n".format(english_the_titles_count))
    f.write("Number of unique terms appearing in the page titles: {}\n".format(unique_terms_count))
    f.write("\n")
    f.write("Title Counts:\n")
    for title, count in title_counts.items():
        f.write("{}: {}\n".format(title, count))
    f.write("\n")
    f.write("Combined Titles:\n")
    for title, data_list in combined_titles.items():
        f.write("{}:\n".format(title))
        for data in data_list:
            f.write("{}\n".format(data))
    # for pair in all_pairs:
    #     f.write("Title: {}, Page 1: {}, Page 2: {}".format(pair[0], pair[1], pair[2]))  

# # Prepare the output as an RDD of strings
# output = [
#     f"Min page size: {min_size}",
#     f"Max page size: {max_size}",
#     f"Average page size: {avg_size}",
#     f"English The titles count: {english_the_titles_count}",
#     f"Number of unique terms appearing in the page titles: {unique_terms_count}",
#     "",
#     "Title Counts:"
# ]
# output += [f"{title}: {count}" for title, count in title_counts.items()]
# output.append("")
# output.append("Combined Titles:")
# for title, data_list in combined_titles.items():
#     output.append(f"{title}:")
#     for data in data_list:
#         output.append(f"{data}")

# output_rdd = sc2.parallelize(output, numSlices=10)
# output_rdd.saveAsTextFile("loop_results_SaveAsTextFile")

# Stop the SparkContext
sc2.stop()

end_time = time.time()
print("Execution time:", end_time - start_time)
total_time_minutes = (end_time - start_time) / 60
print("Execution time in minutes:", total_time_minutes)
# Execution time: 65.48570394515991
# Execution time in minutes: 1.0914283990859985