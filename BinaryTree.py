from anytree import Node, RenderTree

# define a dictionary of materialized views
mviews = {
    "mv1": {"source": "table1", "columns": ["col1", "col2", "col3"]},
    "mv2": {"source": "table2", "columns": ["col4", "col5"]},
    "mv3": {"source": "mv1 JOIN mv2 ON mv1.col1 = mv2.col4", "columns": ["col1", "col2", "col3", "col4", "col5"]}
}

# create the root node
root = Node("Materialized Views")

# create a child node for each materialized view
for name, properties in mviews.items():
    view_node = Node(name, parent=root, source=properties["source"], columns=properties["columns"])

# print the tree
for pre, fill, node in RenderTree(root):
    print("%s%s" % (pre, node.name))
    if node.is_leaf:
        print("%s Source: %s" % (fill, node.source))
        print("%s Columns: %s" % (fill, node.columns))
