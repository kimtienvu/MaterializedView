import treelib
import datetime

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
    tree.create_node(root_id, root_id)

    # Recursively add the left and right subtrees
    add_subtree(tree, nodes[:mid], root_id, "left")
    add_subtree(tree, nodes[mid+1:], root_id, "right")

    return tree

def add_subtree(tree, nodes, parent_id, direction):
    """
    Recursively adds a subtree to the parent node.
    """
    if not nodes:
        return

    # Calculate the index of the middle element
    mid = len(nodes) // 2

    # Add the middle element as a child node
    child_id = nodes[mid]
    child_id_with_direction = parent_id + direction + '_' + str(child_id)
    child1 = tree.create_node(child_id, child_id_with_direction, parent=parent_id)

    # Add data to node being added
    # NOT TESTED: getting the size, frequency of each update, and estimation for execution cost of the material view
    '''
    # If works, make this a function b/c root needs this too
    cursor = connection.cursor()
    cursor.execute("SELECT owner, segment_name, bytes FROM dba_segments WHERE segment_name = 'INSERT NAME OF MV HERE TODO' AND segment_type = 'MATERIALIZED VIEW'")
    size_data = cursor.fetchone()
    size = size_data[2]

    cursor.execute("SELECT last_refresh_date FROM user_mviews WHERE mview_name = 'insert mv name TODO'");
    freq_data = cursor.fetchone()
    temp_freq = freq_data[0]

    #calculate frequency of updates
    time_elapsed = datetime.datetime.now() - last_refresh_date
    days_elapsed = time_elapsed.days
    frequency = 1.0 / days_elapsed

    #Estimate the cost of executing a materialized view
    cursor.execute("BEGIN DBMS_MVIEW.EXPLAIN_MVIEW('INSERT NAME OF MV HERE TODO'); END;")
    statistics = cursor.fetchall()

    # Extract execution statistics: cost is estimated cost of executing the mv, cardinality is the estimated number of rows returned by the mv
    for row in statistics:
        if row[0] == 'Cost':
            cost = row[1]
        elif row[0] == 'Cardinality':
            num_rows = row[1]
    '''
    size = 1024
    frequency = 10
    cost = 8
    child1.data = {"size":size, "frequency":frequency, "exec_cost":cost}

    print(child1.data["size"])

    # Recursively add the left and right subtrees
    add_subtree(tree, nodes[:mid], child_id_with_direction, "left")
    add_subtree(tree, nodes[mid+1:], child_id_with_direction, "right")

# Example usage
nodes = ["MV1", "MV2", "MV3", "MV4", "MV5", "MV6"]
tree = create_balanced_tree(nodes)
tree.show()


# TODO: Use dynamic programming to traverse the mv tree and calculate the optimal one

# After getting optimal one from tree, execute MV?
