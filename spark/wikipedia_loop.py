from pyspark import SparkConf, SparkContext
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

# Loop over each line of data
for line in lines:
    fields = line.split()
    
    # Check if the line has the expected number of fields
    if len(fields) < 4:
        continue  # Skip this line if it doesn't have enough fields
    
    project = fields[0]
    title = fields[1]
    
    # Handle error while converting hits to integer
    try:
        hits = int(fields[2])
    except ValueError:
        print("Error converting hits to integer in line:", line)
        continue  # Skip this line if hits cannot be converted to integer
    
    # Handle error while converting size to integer
    try:
        size = int(fields[3])
    except ValueError:
        print("Error converting size to integer in line:", line)
        continue  # Skip this line if size cannot be converted to integer

    # Update min and max page sizes
    min_size = min(min_size, size)
    max_size = max(max_size, size)
    total_size += size
    total_count += 1

    # Count English titles that start with "The"
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

# Compute average page size
avg_size = total_size / total_count

# Print results
print("Min page size:", min_size)
print("Max page size:", max_size)
print("Average page size:", avg_size)
print("English The titles count:", english_the_titles_count)
print("Number of unique terms appearing in the page titles:", unique_terms_count)

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

# Stop the SparkContext
sc2.stop()

end_time = time.time()
print("Execution time:", end_time - start_time)
total_time_minutes = (end_time - start_time) / 60
print("Execution time in minutes:", total_time_minutes)
# Execution time: 61.45521545410156
# Execution time in minutes: 1.0242535909016928