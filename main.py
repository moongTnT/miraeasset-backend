from datetime import datetime
from dateutil.relativedelta import relativedelta
from core.get_strategy import get_user_strategy

import pandas as pd
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
import uvicorn

from models import StrategyModel

from data.fetch_data import fetch_theme_info, fetch_index_info
from data.get_data import get_pdf_df, get_prices_df

from core.get_weigh import get_cap_weigh, get_bdd_cap_weigh

origins = ["*"]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/theme_info")
def get_theme_info():
    return fetch_theme_info()

@app.get("/pdf_info")
def get_pdf_info(etf_tkr: str = "BKCH"):
    pdf_df = get_pdf_df(etf_tkr=etf_tkr)
    
    tickers = pdf_df['child_stk_tkr'].to_list()
    
    child_prices = get_prices_df(tickers=tickers,
                                 start_date=datetime.now() - relativedelta(years=1))
    
    mkw_weigh = get_cap_weigh(child_prices=child_prices,
                              pdf_df=pdf_df)
    
    upper_bound = fetch_index_info(etf_tkr=etf_tkr)
    
    
    bdd_mkw_weigh = get_bdd_cap_weigh(child_prices=child_prices,
                                      pdf_df=pdf_df,
                                      upper_bound=upper_bound[0]['upper_bound'])
    
    weighs = pd.DataFrame()

    weighs['mkw_weigh'] = mkw_weigh.iloc[-1].round(4)
    weighs['umkw_weigh'] = bdd_mkw_weigh.iloc[-1].round(4)
    weighs['eql_weigh'] = 1/weighs.shape[0]
    weighs['eql_weigh'] = weighs['eql_weigh'].round(4)
    weighs = weighs.reset_index().rename(columns={'index': 'child_stk_tkr'})
    weighs['child_stk_tkr'] = weighs['child_stk_tkr'].str.upper()
    
    tkr_to_name = pdf_df[['child_stk_tkr', 'child_stk_name']]
    
    weighs = weighs.merge(tkr_to_name, on='child_stk_tkr', how='left')
    
    weighs.sort_values(by="mkw_weigh", inplace=True, ascending=False)
    
    weighs.reset_index(drop=True, inplace=True)
    
    return weighs.T.to_dict()

@app.post("/strategy")
def post_strategy(strategy: StrategyModel):
    response = {}
    
    start_date = datetime.now() - relativedelta(years=1)
    
    user_strategy = get_user_strategy(user_config=strategy, start_date=start_date)
    
    # 01 myEtfYtd
    
    ytd_series = user_strategy._get_series(freq=None).loc[start_date:].rebase()
    
    ytd = ytd_series[strategy.myEtfName].reset_index().rename(columns={"index": "date", strategy.myEtfName: "ytd"})
    
    ytd["date"] = ytd["date"].apply(lambda x: str(x).split(' ')[0])
    
    response["date"] = ytd["date"].to_list()
    response["myEtfYtd"] =  ytd["ytd"].to_list()
    
    # 02 myEtfDeposit
    rebalance = user_strategy.get_weights().loc[start_date:].drop(strategy.myEtfName, axis=1)
    rebalance.columns = rebalance.columns.str.replace(strategy.myEtfName+">", "")
    
    myEtfDeposit = {}
    
    for col in rebalance.columns:
        myEtfDeposit[col]  = rebalance[col].to_list()
        
    response["myEtfDeposit"] = myEtfDeposit
    
    # 03 myEtfDrawdown
    drawdown = user_strategy._get_series(freq=None).loc[start_date:].to_drawdown_series()
    response["myEtfDrawdown"] = drawdown[strategy.myEtfName].to_list()
    
    return response

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)