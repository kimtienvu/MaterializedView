# Authors: Kim Tien Vu, Jaskirat Singh Nandhra
# Class: CS 666 - Distributed Systems
# File description: This file defines several functions to create a balanced binary tree and compares the execution time of running the random walk algorithm to using a min-heap to find the node with the optimal query processing cost. See resources below to see how code was adapted for this project. 
#                   Input: List of materialized views of a given query
#                   Output: Optimal materialized view
# Resources used: https://www.programiz.com/dsa/heap-data-structure, https://www.programiz.com/dsa/binary-search-tree

#import mysql.connector
from time import time

# Uncomment this if you have a mysql database connection, otherwise program will dynamically create random data to use
'''
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Vista@701",
  database="testing"
)
mycursor = mydb.cursor()
tic = time()
mycursor.execute("SELECT name FROM 2011_rankings where scores_international_outlook > 20 ")
results = mycursor.fetchall()
toc = time()
# print the results
print('M1 ' + str(toc - tic))
M1=[]
M1T=toc - tic
# Print the results
for result in results:
  M1+=[result]

tic2 = time()
mycursor.execute("SELECT name FROM 2011_rankings WHERE scores_international_outlook > 20 ORDER BY RAND(); ")
result2 = mycursor.fetchall()
toc2 = time()
print('M2 ' + str(toc2 - tic2))
M2T=toc2 - tic2

M2=[]
# Print the results
for result in result2:
  M2+=[result]
tic3 = time()
mycursor.execute("SELECT name FROM 2011_rankings WHERE scores_international_outlook > 20 ORDER BY name; ")
result3 = mycursor.fetchall()
toc3 = time()
M3T=toc3 - tic3

print('M3 ' + str(toc3 - tic3))
M3=[]
# Print the results
for result in result3:
  M3+=[result]
tic4 = time()
mycursor.execute("SELECT name FROM 2011_rankings WHERE scores_international_outlook > 20 group by location; ")
result4 = mycursor.fetchall()
toc4 = time()
M4T=toc4 - tic4

print('M4 ' + str(toc4 - tic4))
M4=[]
# Print the results
for result in result4:
  M4+=[result]
tic5 = time()
mycursor.execute("SELECT name FROM 2011_rankings WHERE scores_international_outlook > 20 group by aliases; ")

result5 = mycursor.fetchall()

toc5 = time()
print('M5 ' + str(toc5 - tic5))
M5T=toc5 - tic5

M5=[]
# Print the results
for result in result5:
  M5+=[result]

tic6 = time()
mycursor.execute("SELECT name FROM 2011_rankings WHERE scores_international_outlook > 20 order by rank_order; ")
result6 = mycursor.fetchall()
toc6 = time()
print('M6 ' + str(toc6 - tic6))
M6=[]
M6T=toc6 - tic6

# Print the results
for result in result6:
  M6+=[result]
'''

import random
import copy

class Node(object):
    def __init__(self, id, val):
        self.id = id
        self.val = val # Query processing cost (block access)
        self.time = 0 # Time to generate the materialized view
        self.left = None
        self.right = None

# code adapted from programiz. See resources at the top
class MinHeap:
    # nodes is a list of nodes
    def __init__(self, nodes):
        self.root = self.build_balanced_min_heap(nodes)

    # nodes is a list of nodes
    def build_balanced_min_heap(self, nodes):

      # Comparator function: prioritize smallest query processing cost, use mv generation time as tie-breaker
      def comparator(node1, node2):
          if node1.val < node2.val:
              return node1.val < node2.val
          elif node1.val == node2.val:
              return node1.time < node2.time
        
      def build_tree(left, right):
          if left > right:
              return None
          mid = (left + right) // 2
          node = nodes[mid]
          node.left = build_tree(left, mid - 1)
          node.right = build_tree(mid + 1, right)
          if node.left and comparator(node.left, node):
              node.left, node = node, node.left
          if node.right and comparator(node.right, node):
              node.right, node = node, node.right
          return node
      return build_tree(0, len(nodes) - 1)

    
# code adapted from programiz. See resources at the top
class BST:
    # nodes is a list of nodes
    def __init__(self, nodes):
        self.root = self.build_BST(nodes)
    
    # Binary search tree to run the random walk algorithm on
    # nodes is a list of nodes
    def build_BST(self, nodes):
      if not nodes:
          return None
      
      mid = len(nodes) // 2
      root = nodes[mid]
      
      # Build the left and right subtrees recursively
      root.left = self.build_BST(nodes[:mid])
      root.right = self.build_BST(nodes[mid+1:])
      return root
    
    # Random Walk tree traversal algorithm
    def random_walk(self):
        node = self.root
        while node:
            if not node.left and not node.right:
                break
            if not node.left:
                node = node.right
            elif not node.right:
                node = node.left
            else:
                node = node.left if random.random() < 0.5 else node.right
        return node

# Step 1: Get a list of all possible materialized views (mv) from a given a query
# Step 1a: Get the block access cost of all possible materialized views. It is measured in terms of the number of records in the materialized view.
# Use this line on 172 if using MySQL database connection: block_access = [len(M1),len(M2),len(M3),len(M4),len(M5),len(M6)]
# Otherwise can still test by dynamically generating a list of all block access costs for each mv
# NOTE THAT BLOCK ACCESS AND BUILD TIME LIST MUST BE THE SAME SIZE!!!
block_access = []
# The range can be higher than 5, it represents the number of materialized views
for i in range(5):
    rand_num = random.random()
    block_access.append(rand_num)

# Step 1b: Get the build time of all possible materialized views. This will be the tie-breaker if the block access costs are the same.
# Use this line on 172 if using MySQL database connection: build_time = [M1T, M2T, M3T, M4T, M5T, M6T]
# dynamically generate a list for the mv generation time
# The range can be higher than 5, it represents the number of materialized views
build_time = []
for i in range(5):
    rand_num = random.random()
    build_time.append(rand_num)

min_val = min(build_time)
max_val = max(build_time)

# Normalize the values in the list
normalized_build_time = list((x - min_val) / (max_val - min_val) for x in build_time)
#print(normalized_build_time)

# Step 2: Generate a list of nodes to build the tree
nodes = []
index = 0
for btime, access in zip(normalized_build_time, block_access):
    node = Node(index, access)
    node.time = btime
    nodes.append(node)
    index = index + 1

# Prints the nodes with their cost and time. Uncomment if you want to see them.
#for n in nodes:
#    print("node has cost of " + str(n.val) + " and time of " + str(n.time))

nodes_copy = copy.deepcopy(nodes)

# Step 3: Build min heap -> This is our contribution
min_heap = MinHeap(nodes) # Min-heap will sort as nodes are inserted into tree

# Step 4: Replicate existing solution. Sort nodes in ascending index order for BST creation (for random walk later) 
sorted_nodes = sorted(nodes_copy, key=lambda node: node.id)
bst = BST(sorted_nodes)

# Step 5: Compare execution times of our approach (min-heap) vs. BST with Random walk existing solution.
# Get execution time of using min-heap to find the optimal mv
tic2 = time()
optimal_node = min_heap.root
toc2 = time()
print('Min heap optimal node: block access cost = ' + str(optimal_node.val) + ', execution time: ' + str(toc2 - tic2) + ' sec')

# Get execution time of random walk algorithm to find the optimal mv
tic = time()
random_node = bst.random_walk()
toc = time()
print('Random walk optimal node: block access cost = ' + str(random_node.val) + ', execution time: ' + str(toc - tic) + ' sec')
