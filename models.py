from typing import List, Dict

from pydantic import BaseModel

dummy_request_body={
    "rebalancePeriod": "3M",
    "rateMethod": "동일가중",
    "myEtfName": "김밍두기요오",
    "myEtfPdf":[
        {"child_stk_tkr": "COIN", "ratio": 0.7},
        {"child_stk_tkr": "MARA", "ratio": 0.2}, 
        {"child_stk_tkr": "IRES", "ratio": 0.1}
    ]
}

class StrategyModel(BaseModel):
    rebalancePeriod: str
    rateMethod: str
    myEtfName: str
    myEtfPdf: List[Dict]