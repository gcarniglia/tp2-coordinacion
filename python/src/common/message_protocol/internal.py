import json

class InternalMessageType:
    GAT_SUM_DATA = 0
    GAT_SUM_EOF = 1
    SUM_EOF_REQ = 2
    SUM_EOF_REP = 3
    SUM_AGG_DATA = 4
    SUM_AGG_EOF = 5
    AGG_JOIN_DATA = 6
    AGG_JOIN_EOF = 7
    JOIN_GAT_DATA = 8


class InternalMessage:

    type : InternalMessageType
    source_client_uuid : str | None
    data : any | None
    
    def __init__(self, type=None, source_client_uuid=None, data=None):
        self.type = type
        self.source_client_uuid = source_client_uuid
        self.data = data

    def _serialize(self):
        msg_dict = {"type": self.type}

        if self.source_client_uuid is not None:
            msg_dict["source_client_uuid"] = self.source_client_uuid

        if self.data is not None:
            msg_dict["data"] = self.data

        return json.dumps(msg_dict).encode("utf-8")
    
    def _deserialize(self, data):
        msg = json.loads(data.decode("utf-8"))
        self.type = msg["type"] if "type" in msg else None
        self.source_client_uuid = msg["source_client_uuid"] if "source_client_uuid" in msg else None
        self.data = msg["data"] if "data" in msg else None


def serialize(type,client_id,data):
    msg = InternalMessage(type=type, source_client_uuid=client_id, data=data)
    return msg._serialize()



def deserialize(data) -> InternalMessage:
    msg = InternalMessage()
    msg._deserialize(data)
    return msg
