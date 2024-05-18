from pyspark import SparkConf, SparkContext
from itertools import combinations
import re
import time


Start_time = time.time()

# Create a SparkContext
conf = SparkConf().setMaster("local").setAppName("PopularWikipediaPagesMapReduce")
sc = SparkContext(conf=conf)

def parseLine(line):
    try:
        fields = line.split()
        project = fields[0]
        title = fields[1]
        hits = int(fields[2])
        size = int(fields[3])
        return (project, title, hits, size)
    except Exception as e:
        # print("Error parsing line:", line)
        # print("Exception:", e)
        return None

# Read the data from the file using parseLine function
lines = sc.textFile("pagecounts-20160101-000000_parsed.out")
rdd = lines.map(parseLine).filter(lambda x: x is not None)


# Compute min, max, and average page sizes
minSize = rdd.map(lambda x: x[3]).min()
maxSize = rdd.map(lambda x: x[3]).max()
avgSize = rdd.map(lambda x: x[3]).mean()
avgSize = round(avgSize, 5)


# Count page titles that start with "The" and are not part of the English project
english_the_titles_count = rdd.filter(lambda x: x[0] != "en" and x[1].startswith("The")).count()


def preprocess_title(title):
    # Lowercase the title
    title = title.lower()
    # Remove non-alphanumeric characters an
    title = re.sub(r'[^a-z0-9_]', '', title)
    return title
preprocessed_titles = rdd.map(lambda x: preprocess_title(x[1]))
terms = preprocessed_titles.flatMap(lambda title: title.split("_"))
unique_terms_count = terms.distinct().count()


# Extract each title and the number of times it was repeated
title_counts = rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y)


# Combine between data of pages with the same title and save each pair of pages data in order to display them
combined_titles = rdd.map(lambda x: (x[1], (x[0], x[2], x[3]))).groupByKey().filter(lambda x: len(x[1]) > 1).mapValues(list)

# # Group pages by their title
# grouped_pages = rdd.groupBy(lambda x: x[1])
# # Function to generate pairwise combinations within each group
# def generate_pairs(group):
#     title = group[0]
#     pages = group[1]
#     page_combinations = combinations(pages, 2)  # Generate all pairwise combinations
#     return [(title, pair[0], pair[1]) for pair in page_combinations]
# # Generate pairwise combinations for each group and flatten the result
# pairs_rdd = grouped_pages.flatMap(lambda group: generate_pairs(group))



with open("map_reduce_results.txt", "w", encoding="utf-8") as f:
    f.write("Min page size: {}\n".format(minSize))
    f.write("Max page size: {}\n".format(maxSize))
    f.write("Average page size: {}\n".format(avgSize))
    f.write("English The titles count: {}\n".format(english_the_titles_count))
    f.write("Number of unique terms appearing in the page titles: {}\n".format(unique_terms_count))
    f.write("\n")
    f.write("Title Counts:\n")
    for title, count in title_counts.collect():
        f.write("{}: {}\n".format(title, count))
    f.write("\n")
    f.write("Combined Titles:\n")
    for title, data_list in combined_titles.collect():
        f.write("{}:\n".format(title))
        for data in data_list:
            f.write("{}\n".format(data))
    # for title, page1, page2 in pairs_rdd.collect():
    #     f.write("Title: {}, Page1: {}, Page2: {}".format(title, page1, page2))

# # Prepare the output as an RDD of strings
# output = [
#     f"Min page size: {minSize}",
#     f"Max page size: {maxSize}",
#     f"Average page size: {avgSize}",
#     f"English The titles count: {english_the_titles_count}",
#     f"Number of unique terms appearing in the page titles: {unique_terms_count}",
#     "",
#     "Title Counts:"
# ]
# output += [f"{title}: {count}" for title, count in title_counts.collect()]
# output.append("")
# output.append("Combined Titles:")
# for title, data_list in combined_titles.collect():
#     output.append(f"{title}:")
#     for data in data_list:
#         output.append(f"{data}")

# output_rdd = sc.parallelize(output, 10)

# # Save the output to a text file
# output_rdd.saveAsTextFile("map_reduce_results_SaveAsTextFile")


sc.stop()  


end_time = time.time()
total_time = end_time - Start_time
print("Total time: ", total_time)
total_time_minutes = total_time / 60
print("Total time in minutes: ", total_time_minutes)
# Total time:  275.6192638874054
# Total time in minutes:  4.593654398123423