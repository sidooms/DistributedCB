#Copyright (C) 2013, Simon Dooms

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import time
from time import strftime, clock
import os
import sys
import glob
import math
import random
import copy
import multiprocessing

def write_logs(logs, log_file):
    file_out = file(log_file, 'a')
    file_out.writelines(logs)
    file_out.close()

def add_log_time_now(message, start_time):
    global logs
    logs.append(str(strftime("%a %b %d %H:%M:%S %Y")) + ',' + message + ',' + str(time.time() - start_time) + '\n' )

def read_movielens_items(input_path):    
    item_indexes = list()
    item_files_ordered = ("movies.dat", "tags.dat")
    items_seen = set()
    for item_file in item_files_ordered:
        file_in = file(input_path + '/' + item_file, 'r')
        lines = file_in.readlines()
        file_in.close()
        begin_time = time.time()   
        for line in lines:
            line_arr = line.split('::') 
            if item_file == 'movies.dat':
                item = int(line_arr[0])
            else:
                item = int(line_arr[1])
            if item not in items_seen:
                item_indexes.append(item)
                items_seen.add(item)
        lines = []
        add_log_time_now('work_division,serial,-1', begin_time)       
    lines = []
    return item_indexes 

def get_item_skips(item, part_of_users, ratings_reverse, ratings_loads):
    item_skips = 0
    try:
        users_who_rated_item = part_of_users.intersection(ratings_reverse[item])
    except: 
        #not a single user has rated this item
        return 0
    for user in users_who_rated_item:
        item_skips += ratings_loads[user]
    return item_skips
            
def read_ratings(ratings_file):
    global logs
    file_in = file(ratings_file, 'r')
    lines = file_in.readlines()
    file_in.close()
    begin_time = time.time()    
    #var init
    ratings_lines = dict() #user => string (of the rating lines)
    ratings_loads = dict() #user => int (number of ratings)
    ratings_reverse = dict() #item => set (users who rated item)
    #parse the rating lines
    for line in lines:
        #line=> uid::itemid::rating::timestamp
        line_arr = line.split('::')
        user = int(line_arr[0])
        item = int(line_arr[1])
        try:
            ratings_lines[user].append(line)
            ratings_loads[user] += 1
        except:
            ratings_lines[user] = [line]
            ratings_loads[user] = 1
        #reverse ratings
        try:
            ratings_reverse[item].add(user)
        except:
            ratings_reverse[item] = set([user])
    lines = [] #allow memory to be freed faster?
    add_log_time_now('work_division,serial,-1', begin_time)    
    return ratings_lines, ratings_loads, ratings_reverse
    
def get_minimum_node(indexes_loads):
    minimum = float('inf') #infinity
    minimum_node = -1
    for node in indexes_loads:
        the_load = indexes_loads[node]
        #this is the poorest
        if the_load == 0:
            return node
        if the_load < minimum:
            minimum = the_load
            minimum_node = node
    return minimum_node   
    
def get_max_min_node(indexes_loads):
    max = -1
    max_node = -1
    min = float('inf') #infinity
    min_node = -1
    for node in indexes_loads:
        node_load = indexes_loads[node]
        if node_load < min:
            min = node_load
            min_node = node
        if node_load > max:
            max = node_load
            max_node = node
    return max_node, min_node
    
def random_split(the_indexes, number_of_parts):
    random.shuffle(the_indexes)
    indexes = dict()
    for node in range(number_of_parts):
        indexes[node] = list()
    #randomly divide across the parts
    for item in the_indexes:
       random_node = random.randint(0,number_of_parts-1)
       indexes[random_node].append(item)
    return indexes   

def robin_hood_split(item_indexes, item_load, number_of_parts, diff_goal=500, max_iterations=500000):
    #sort items according load
    item_indexes_sorted = list()
    for item in item_indexes:
        item_indexes_sorted.append((item_load[item],item))
    item_indexes_sorted.sort(reverse=True)    
    #initialize vars
    iterations = 0
    minimum_indexes = dict()
    indexes = dict()
    indexes_loads = dict()
    for node in range(number_of_parts):
        indexes[node] = list()
        indexes_loads[node] = 0
    log_them_all = ''
    #Divide the items over the poor
    for couple in item_indexes_sorted:
       the_load = couple[0]
       the_item = couple[1]
       node = get_minimum_node(indexes_loads)
       indexes[node].append(the_item)
       indexes_loads[node] += the_load
    minimum_indexes = copy.deepcopy(indexes)
    #calculate the difference of this situation
    vals = indexes_loads.values()
    diff = max(vals) - min(vals)
    min_diff = diff
    #Redistribute work in 'robin hood mode'
    while  diff > diff_goal and iterations < max_iterations:
        #select the richest and poorest node
        rich, poor = get_max_min_node(indexes_loads)
        #pick something from the rich ...
        something = indexes[rich].pop(random.randint(0,len(indexes[rich])-1))
        #... and give it to the poor
        indexes[poor].append(something)
        #correct the load counters
        some_load = item_load[something]
        indexes_loads[rich] -= some_load
        indexes_loads[poor] += some_load
        vals = indexes_loads.values()
        diff = max(vals) - min(vals)
        if diff <= min_diff:
            #store minimum results
            min_diff = diff
            #minimum_indexes = copy.deepcopy(indexes)
        iterations += 1
        log_them_all += str(iterations) + ',' + str(min_diff) + '\n'
    return minimum_indexes
   
def calc_item_load(item_indexes, part_of_users, ratings_reverse, ratings_loads):
    num_ratings = sum([ratings_loads[user] for user in part_of_users])
    item_load = dict()
    for item in item_indexes:
        item_load[item] = num_ratings - get_item_skips(item, part_of_users, ratings_reverse, ratings_loads)
    return item_load  

def write_user_division(user_division, ratings_lines, output_path, base_file_name):
    for chunk in user_division:
        lines = list()
        for user in user_division[chunk]:
            lines += ratings_lines[user]

        file_out = file(output_path + '/' + base_file_name + '_' + str(chunk), 'w')
        file_out.writelines(lines)
        file_out.close()
        
def write_item_division(item_division, output_path, base_file_name, userjob): 
    for chunk in item_division:
        lines = list()
        for item in item_division[chunk]:
            lines.append(str(item) + '\n')
        file_out = file(output_path + '/' + base_file_name + '_'+ str(userjob) + '_' + str(chunk), 'w')
        file_out.writelines(lines)
        file_out.close()
                
def make_all_as_one(ratings_loads):
    for user in ratings_loads:
        ratings_loads[user] = 1
    return ratings_loads
        

def parallel_item_division(userjob):    
    if recommender_split_item_data == 'byiteration':
        item_load = calc_item_load(item_indexes, set(user_division[userjob]), ratings_reverse, ratings_loads)
        item_division = robin_hood_split(item_indexes, item_load, number_of_item_jobs, robin_hood_min_diff, robin_hood_max_iterations)
    elif recommender_split_item_data == 'byitem':
        item_load = dict()
        for item in item_indexes:
            item_load[item] = 1
        random.shuffle(item_indexes)
        item_division = robin_hood_split(item_indexes, item_load, number_of_item_jobs, 2, 1000000)    
    elif recommender_split_item_data == 'random':
        item_division = random_split(item_indexes, number_of_item_jobs)
    write_item_division(item_division, output_path, 'item_indexes.dat', userjob)
        

if __name__ == "__main__":    
    input_path = str(sys.argv[1])
    output_path = str(sys.argv[2])
    number_of_user_jobs = int(sys.argv[3])
    number_of_item_jobs = int(sys.argv[4])
    number_of_cores = int(sys.argv[5])
    log_file = str(sys.argv[6])
    recommender_split_user_data = 'byrating' #can also be 'byuser' or 'random'
    recommender_split_item_data = 'byiteration' #can also be 'byitem' or 'random'
    if len(sys.argv) > 8:
        robin_hood_min_diff = int(sys.argv[7])
        robin_hood_max_iterations = int(sys.argv[8])
    else: #default values for backwards compatibility
        robin_hood_min_diff = 5
        robin_hood_max_iterations = 10000000   
    #list containing log messages
    logs = list()    
    #----------------------
    # User ratings division
    #----------------------
    #logging inside this function (to split out network activity time)
    ratings_lines, ratings_loads, ratings_reverse = read_ratings(input_path + '/ratings.dat')
    begin_time = time.time()    
    if recommender_split_user_data == 'byrating':
        user_division = robin_hood_split(ratings_lines.keys(), ratings_loads, number_of_user_jobs, 1, 500000)
    elif recommender_split_user_data == 'byuser':
        ratings_loads = make_all_as_one(ratings_loads)
        user_keys = ratings_lines.keys()
        random.shuffle(user_keys)
        user_division = robin_hood_split(user_keys, ratings_loads, number_of_user_jobs, 1, 500000)
    elif recommender_split_user_data == 'random':
        user_division = random_split(ratings_lines.keys(), number_of_user_jobs)
    add_log_time_now('work_division,serial,-1', begin_time)   
    write_user_division(user_division, ratings_lines, output_path, 'ratings.dat')   
    #cleanup no longer needed vars (maybe free memory?)
    ratings_lines = []
    #-------------------
    # Item data division
    #-------------------
    #logging inside this function (to split out network activity time)
    item_indexes = read_movielens_items(input_path)
    begin_time = time.time()    
    #
    #parallel way
    #
    #pool =  multiprocessing.Pool(number_of_cores)
    #pool.map(parallel_item_division, user_division, 1)
    #pool.close()
    #
    #serial way
    #
    for userjob in user_division:
        parallel_item_division(userjob)
    add_log_time_now('work_division,serial,-1', begin_time)   
    write_logs(logs, log_file)