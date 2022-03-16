

import json
from NeighbourNode import NeighbourNode

class NodeState:

    def __init__(self,own=None,succ=None,pred=None):
        self.own = own
        self.succ = succ
        self.pred = pred
    
    # def __repr__():

    def serialize(self):
        return {
            "own":self.own and self.own.serialize(),
            "succ":self.succ and self.succ.serialize(),
            "pred":self.pred and self.pred.serialize()
        }
    
    def __repr__(self):
        succ = 'SUCC Node {nodeId}'.format(nodeId=self.succ.id)
        pred = 'PRED Node {nodeId} '.format(nodeId=self.pred.id) if self.pred != None else None

        return """
            Node {nodeKey}
            {succ}
            {pred}
        """.format(succ=succ,pred=pred,nodeKey=self.own.id)

    def deserialize(self,doc):
        obj = json.loads(doc)
        own = obj.get("own") and obj.get("own")

        ownNode=None
        succNode=None
        predNode=None

        if own is not None:
            ownObj = json.loads(own)
            ownNode = NeighbourNode(
                host=ownObj["host"],
                port=ownObj["port"],
                id=ownObj["id"]
            )
        succ = obj.get("succ") and obj.get("succ")
        if succ is not None:
            succObj = json.loads(succ)
            succNode = NeighbourNode(
                host=succObj["host"],
                port=succObj["port"],
                id=succObj["id"]
            )
        pred = obj.get("pred") and obj.get("pred")
        if pred is not None:
            predObj = json.loads(pred)
            predNode = NeighbourNode(
                host=predObj["host"],
                port=predObj["port"],
                id=predObj["id"]
            )
        self.own = ownNode
        self.succ = succNode
        self.pred = predNode






