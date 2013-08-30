#Copyright (C) 2013, Simon Dooms

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import time
from time import strftime, clock
import os
import sys
import math
import string
import random
import re
import multiprocessing

def write_logs(logs, log_file):
    file_out = file(log_file, 'a')
    file_out.writelines(logs)
    file_out.close()

def add_log_time_now(message, start_time):
    global logs
    logs.append(str(strftime("%a %b %d %H:%M:%S %Y")) + ',' + message + ',' + str(time.time() - start_time) + '\n' )
    
def get_similarity(simils, item, rated_item):
    global cache_hits, cache_misses 
    #check if already calculated
    if item < rated_item:
        small = item
        larger = rated_item
    else:
        small = rated_item
        larger = item
    key = str(small) + '_' + str(larger)
    try:
        #if simil has been calculated before
        similarity = simils[key]
        cache_hits += 1
    except:
        #if it should be calculated
        similarity = calculate_item_similarity(small, larger)
        simils[key] = similarity
        cache_misses += 1
    return similarity

def calculate_recommendation(item, user, simils, ratings):
    vote = 0
    weights = 0    
    item = int(item)
    user = int(user)
    for rated_item, rating in ratings[user].iteritems():    
        weight = get_similarity(simils, item, rated_item)
        vote += (weight * rating)
        weights += weight
    if weights == 0:
        weights = 1
    rec = vote / weights
    return rec
  
def calculate_item_similarity(item_1,item_2):
    #Jaccard similarity |A n B|/|A u B|
    item_1_data = item_data[item_1]
    item_2_data = item_data[item_2]
    return len(item_1_data.intersection(item_2_data)) / float(len(item_1_data.union(item_2_data)))
    
def process_tag_data(line_arr):
    item = int(line_arr[1])
    tag = line_arr[2]
    tags = list()
    tags.append(tag)
    return item, tags
    
def process_genre_data(line_arr):
    item = int(line_arr[0])
    genres = line_arr[2].split('|')
    return item, genres

def read_movielens_item_data(input_path,all_job_number):    
    item_data_files = {"movies.dat" : process_genre_data, "tags.dat": process_tag_data}
    item_data = dict()
    item_indexes = list()    
    item_files_ordered = ("movies.dat", "tags.dat")
    for item_file in item_files_ordered:
        file_in = file(input_path + '/' + item_file, 'r')
        lines = file_in.readlines()
        file_in.close()
        begin_time = time.time()
        for line in lines:
            #Process the line with the function associated with the file
            line_arr = line.split('::') 
            item, data = item_data_files[item_file](line_arr)  
            if item_data.get(item) == None:
                item_data[item] = set()
                item_indexes.append(item)
            while data:
                item_data[item].add(data.pop())
        add_log_time_now('content_based,serial,'+ str(all_job_number), begin_time)
        lines = []
    lines = []
    return item_data, item_indexes 
    
def read_ratings(ratings_file,all_job_number):
    file_in = file(ratings_file, 'r')
    lines = file_in.readlines()
    file_in.close()
    begin_time = time.time()
    num_ratings = len(lines)
    ratings = dict()
    #line=> uid::itemid::rating::timestamp
    for line in lines:
        line_arr = line.split('::')
        user = int(line_arr[0])
        item = int(line_arr[1])
        rating = float(line_arr[2]) 
        try:
            ratings[user][item] = rating
        except:    
            ratings[user] = {item : rating}
    add_log_time_now('content_based,paralleluser,'+ str(all_job_number), begin_time)
    lines = []
    return ratings, num_ratings
  
def get_item_node_indexes(ratings_file, node, all_job_number):
    file_in = file(string.replace(ratings_file, 'ratings.dat', 'item_indexes.dat') + '_' + str(node), 'r')
    lines = file_in.readlines()
    file_in.close()
    indexes = list()
    begin_time = time.time()
    for line in lines:
        indexes.append(int(line.strip()))
    add_log_time_now('content_based,parallelitem,'+ str(all_job_number), begin_time)        
    return indexes    

def parallel_calculate_recommendations_for_item(item):
    global cache_hits, cache_misses
    cache_hits = 0
    cache_misses = 0
    item_similarities = dict()
    #for all users
    for user in ratings.iterkeys():           
        if item in ratings[user]:
            continue #skip already rated items    
        rec = calculate_recommendation(item, user, item_similarities, ratings)       
    return (cache_hits, cache_misses)

if __name__ == "__main__":        
    #Get the input parameters
    all_job_number = int(sys.argv[1])
    number_of_cores = int(sys.argv[2])
    input_path = str(sys.argv[3])
    output_path = str(sys.argv[4])
    ratings_file = str(sys.argv[5])
    number_of_user_jobs = int(sys.argv[6])
    number_of_item_jobs = int(sys.argv[7])
    log_file = str(sys.argv[8])
    logs = list()    
    begin_time = time.time()     
    all_number_of_jobs = number_of_item_jobs * number_of_user_jobs
    item_job_number = int(math.floor(all_job_number / (math.floor(all_number_of_jobs / number_of_item_jobs))))
    ratings_file_index = all_job_number % number_of_user_jobs
    ratings_file += '_' + str(ratings_file_index)
    add_log_time_now('content_based,serial,'+ str(all_job_number), begin_time)
    #read all input data (these functions do their own logging)
    ratings, num_ratings = read_ratings(ratings_file,all_job_number)
    item_data, item_indexes = read_movielens_item_data(input_path,all_job_number)
    work = get_item_node_indexes(ratings_file, item_job_number, all_job_number)
    begin_time = time.time()     
    cache_hits = 0
    cache_misses = 0
    pool =  multiprocessing.Pool(number_of_cores)
    #chunksize is 1 item
    iterations = list()
    #
    #serial way
    #
    #for item in work:
    #    iterations.append(parallel_calculate_recommendations_for_item(item))
    #
    #parallel way
    #
    iterations = pool.map(parallel_calculate_recommendations_for_item, work) 
    add_log_time_now('content_based,parallelnodecore,'+ str(all_job_number), begin_time)   
    pool.close()
    #log the iterations
    cache_hits = 0
    cache_misses = 0
    for iter in iterations:
        cache_hits += iter[0]
        cache_misses += iter[1]
    logs.append(str(strftime("%a %b %d %H:%M:%S %Y")) + ',' + 'metrics,cache_hits,'+ str(all_job_number) + ',' + str(cache_hits) + '\n')
    logs.append(str(strftime("%a %b %d %H:%M:%S %Y")) + ',' + 'metrics,cache_misses,'+ str(all_job_number) + ',' + str(cache_misses) + '\n')
    logs.append(str(strftime("%a %b %d %H:%M:%S %Y")) + ',' + 'metrics,users,'+ str(all_job_number) + ',' + str(len(ratings)) + '\n')
    logs.append(str(strftime("%a %b %d %H:%M:%S %Y")) + ',' + 'metrics,items,'+ str(all_job_number) + ',' + str(len(work)) + '\n')
    logs.append(str(strftime("%a %b %d %H:%M:%S %Y")) + ',' + 'metrics,ratings,'+ str(all_job_number) + ',' + str(num_ratings) + '\n')
    write_logs(logs, log_file)