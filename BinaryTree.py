# NOTE: Need to pip install treelib
import treelib

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
    # Needs to be a string!
    child_id_with_direction = parent_id + direction + '_' + str(child_id)
    tree.create_node(child_id, child_id_with_direction, parent=parent_id)

    # Recursively add the left and right subtrees
    add_subtree(tree, nodes[:mid], child_id_with_direction, "left")
    add_subtree(tree, nodes[mid+1:], child_id_with_direction, "right")

# Each node in the tree should represent a materialized view
nodes = ["SELECT * FROM TABLEA", "SELECT * FROM TABLEB"]
tree = create_balanced_tree(nodes)
tree.show()

# TODO: Use dynamic programming to traverse the materialized view tree and choose the optimal one

# After getting the optimal one from the tree, execute the materialized view???
