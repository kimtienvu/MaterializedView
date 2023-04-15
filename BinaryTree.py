# Authors: Kim Tien Vu, Jaskirat Singh Nandhra
# Class: CS 666 - Distributed Systems
# File description: This file defines several functions to create a balanced binary tree and a dynamic programming algorithm to find the node with the smallest size, frequency, and cost. 
#                   Input: List of materialized views of a given query
#                   Output: Optimal materialized view

# You will need to `pip install treelib` if you haven't gotten it installed yet
import treelib
import datetime

# Keeps track of the weights in each materialized view
class metadata(object):
    def __init__(self, id, size, frequency, cost):
        self.id = id
        self.size = size # size of the materialized view (mv)
        self.frequency = frequency # frequency of updates on each mv
        self.cost = cost # Estimated query execution cost on each mv

# TODO: a function that finds the weights for each mv
# Input: id - is the index of the mv in the Input list all the way at the bottom
#       mv_name - the materialized view name in a string format
def set_metadata(id, mv_name):

    # CODE COMMENTED BELOW IS NOT TESTED: 
    # we want to get the size of each mv, frequency of each update, and estimation for execution cost of the material view
    '''
    # TODO: calculate the size of each mv
    # https://stackoverflow.com/questions/62589909/how-to-get-the-size-of-a-materialized-view-in-oracle
    # Or use ESTIMATE_MVIEW_SIZE procedure: https://docs.oracle.com/database/121/ARPLS/d_mview.htm#ARPLS67189
    cursor = connection.cursor()
    cursor.execute("SELECT owner, segment_name, bytes FROM dba_segments WHERE segment_name = 'TODO: can we put variables in here: mv_name' AND segment_type = 'MATERIALIZED VIEW'") 
    size_data = cursor.fetchone()
    size = size_data[2]
    
    # TODO: calculate the frequency of updates
    # Get the last refresh date: https://stackoverflow.com/questions/5798894/materialized-views-identifying-the-last-refresh
    cursor.execute("SELECT owner, mview_name, last_refresh_date FROM all_mviews WHERE mview_name = 'TODO: insert mv name'");
    freq_data = cursor.fetchone()
    temp_freq = freq_data[2]

    # calculate frequency of updates formula: current time - last_refresh_date => convert to # of days then do 1/#of days to get frequency of updates
    time_elapsed = datetime.datetime.now() - temp_freq
    days_elapsed = time_elapsed.days
    frequency = 1.0 / days_elapsed

    # TODO: Estimate the cost of executing a materialized view -> or get rid of this and get the actual time once we choose an optimal mv. Because in theory, less search space = improved time
    does oracle db have an execution timer for cpu or i/o????
    # DBMS_XPLAN.DISPLAY: https://docs.oracle.com/database/121/ARPLS/d_xplan.htm#ARPLS378
    # Uses an estimated cost of each step in the query plan calculated by the Oracle-Cost-Based Optimizer expressed in arbitrary units.
    '''
    size = 0
    frequency = 0
    cost = 0

    return metadata(id, size, frequency, cost)

def create_balanced_tree(nodes):
    """
    Creates a balanced binary tree with assigned IDs from a list of nodes.
    """
    # Create an empty tree
    tree = treelib.Tree()

    # Calculate the index of the middle element
    mid = len(nodes) // 2

    # Add the middle element as the root node
    root_id = nodes[mid]
    # TODO: check to make sure metadata matches!
    root_metadata = set_metadata(mid, root_id)
    tree.create_node(root_id, root_id, data = root_metadata)

    # Recursively add the left and right subtrees
    add_subtree(tree, nodes[:mid], root_id)
    add_subtree(tree, nodes[mid+1:], root_id)

    return tree

def add_subtree(tree, nodes, parent_id):
    """
    Recursively adds a subtree to the parent node.
    """
    if not nodes:
        return

    # Calculate the index of the middle element
    mid = len(nodes) // 2

    # Add the middle element as a child node
    child_id = nodes[mid]
    child1 = tree.create_node(child_id, child_id, parent=parent_id)

    # Add data to node being added
    node_metadata = set_metadata(mid, child_id)
    child1.data = node_metadata

    # Recursively add the left and right subtrees
    add_subtree(tree, nodes[:mid], child_id)
    add_subtree(tree, nodes[mid+1:], child_id)


def find_optimal_node(tree):
    
    # Ensures we get a root
    tree.root = tree.get_node(tree.root)
    
    # Initialize memoization table
    memo = {}
    min_cost_node = tree.root

    # Define recursive function to find optimal node
    def traverse(node, min_cost_node):

        #print( "0. currently on node: " + str(node.identifier) + " , cost: " + str(node.data.cost))

        # If node is None, stop
        if node is None:
            return None

        # Base case: node is a leaf
        if len(tree.children(node.identifier)) == 0:
            return node

        # Check if we've already computed the optimal node for this subtree
        if node.identifier in memo:
            return memo[node.identifier]

        # Traverse all children of this node
        for child_id in tree.children(node.identifier):
            #print("-----------child_id is " + str(child_id.identifier))
            child = tree.get_node(child_id.identifier)
            #print("child is " + str(child.identifier) + " cost: " + str(child.data.cost))

            # Recursively find the optimal node in the child subtree
            current_child = traverse(child, min_cost_node)
            #print("current child is " + str(current_child.identifier) + " cost: " + str(current_child.data.cost))

            # Choose the node with the smallest sum of all the weights
            if min_cost_node is None or (current_child.data.cost + current_child.data.size + current_child.data.frequency <= min_cost_node.data.cost + min_cost_node.data.size + min_cost_node.data.frequency):
                min_cost_node = current_child
        
        # Memoize the result
        memo[node.identifier] = min_cost_node

        # Return the optimal node for this subtree
        return min_cost_node

    # Call the recursive function on the root node
    return traverse(tree.root, min_cost_node)

# Example usage -> RIGHT NOW ROOT IS OPTIMAL BECAUSE ALL WEIGHTS ARE 0
# TODO: Connect to Oracle and get a list of all possible materialized views given a query
nodes = ["MV1", "MV2", "MV3", "MV4", "MV5", "MV6"]
tree = treelib.Tree()
tree = create_balanced_tree(nodes)
# Print out the tree
tree.show()

# Use dynamic programming to traverse the mv tree and find the optimal one
optimal_node = find_optimal_node(tree)
print("Optimal node: " + optimal_node.identifier)

# TODO: After getting optimal mv from tree, execute query on MV and calculate execution time using Python timing library
#       We need to compare runtimes with the ones in the paper
