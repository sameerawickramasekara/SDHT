
def inRange(node,succesor,id):
    if(node < succesor):
        return node < id and id <= succesor
    return node < id or id<=succesor