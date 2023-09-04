from datetime import datetime
from dateutil.relativedelta import relativedelta
from core.get_strategy import get_user_strategy

import pandas as pd
import bt
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
import uvicorn

from models import StrategyModel, ClickInvestModel

from data.fetch_data import fetch_theme_info, fetch_index_info
from data.get_data import get_pdf_df, get_prices_df

from core.get_weigh import get_cap_weigh, get_bdd_cap_weigh
from core.get_backtest import get_eql_backtest, get_mkw_backtest, get_bdd_mkw_backtest

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
    
    upper_bound = fetch_index_info(etf_tkr=etf_tkr)[0]['upper_bound']
    
    bdd_mkw_weigh = get_bdd_cap_weigh(child_prices=child_prices,
                                      pdf_df=pdf_df,
                                      upper_bound=upper_bound)
    
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

@app.get("/dist_methology")
def get_dist_methology(etf_tkr: str = "BKCH"):
    
    pdf_df = get_pdf_df(etf_tkr=etf_tkr)
    
    tickers = pdf_df['child_stk_tkr'].to_list()
    
    child_prices = get_prices_df(tickers=tickers,
                                 start_date=datetime.now() - relativedelta(years=1))
    
    eql_backtest = get_eql_backtest(name="동일가중", child_prices=child_prices)
    
    mkw_weigh = get_cap_weigh(child_prices=child_prices, pdf_df=pdf_df)
    mkw_backtest = get_mkw_backtest(name="시총가중", child_prices=child_prices, weigh=mkw_weigh)
        
    upper_bound = fetch_index_info(etf_tkr=etf_tkr)[0]['upper_bound']
    bdd_weigh = get_bdd_cap_weigh(child_prices=child_prices, pdf_df=pdf_df, upper_bound=upper_bound)
    bdd_backtest = get_bdd_mkw_backtest(name="ETF방식그대로", child_prices=child_prices, weigh=bdd_weigh)
    
    ret = bt.run(eql_backtest, mkw_backtest, bdd_backtest)
    
    date_list = ret._get_series(freq=None).index.to_list()
    eql = ret._get_series(freq=None)['동일가중'].to_list()
    mkw = ret._get_series(freq=None)['시총가중'].to_list()
    bdd = ret._get_series(freq=None)['ETF방식그대로'].to_list()
    
    ret_json = {
        "date": date_list,
        "동일가중": eql,
        "시총가중": mkw,
        "ETF방식그대로": bdd
    }
    
    return ret_json

@app.post("/click_invest")
def post_click_invest(user_info: ClickInvestModel):
    return "hello!"
    

@app.post("/strategy")
def post_strategy(strategy: StrategyModel):
    response = {}
    
    start_date = datetime.now() - relativedelta(years=1)
    
    upper_bound = fetch_index_info(etf_tkr=strategy.myEtfTkr)[0]['upper_bound']
    
    user_strategy = get_user_strategy(user_config=strategy,
                                      start_date=start_date,
                                      upper_bound=upper_bound)
    
    # 01 myEtfYtd
    
    ytd_series = user_strategy._get_series(freq=None).loc[start_date:].rebase()
    
    ytd = ytd_series[strategy.myEtfName].reset_index().rename(columns={"index": "date", strategy.myEtfName: "ytd"})
    
    ytd["date"] = ytd["date"].apply(lambda x: str(x).split(' ')[0])
    
    response["date"] = ytd["date"].to_list()
    response["myEtfYtd"] =  round(ytd["ytd"], 2).to_list()
    
    # 02 myEtfDeposit
    rebalance = user_strategy.get_weights().loc[start_date:].drop(strategy.myEtfName, axis=1)
    rebalance.columns = rebalance.columns.str.replace(strategy.myEtfName+">", "")
    
    myEtfDeposit = {}
    
    for col in rebalance.columns:
        myEtfDeposit[col]  = round(rebalance[col], 3).to_list()
        
    response["myEtfDeposit"] = myEtfDeposit
    
    # 03 myEtfDrawdown
    drawdown = round(user_strategy._get_series(freq=None).loc[start_date:].to_drawdown_series(), 3)
    response["myEtfDrawdown"] = drawdown[strategy.myEtfName].to_list()
    
    return response

from uvicorn.config import LOGGING_CONFIG, Config
import uvicorn
import logging

if __name__ == "__main__":
    DATE_FMT = "%Y-%m-%d %H:%M:%S"

    # Modify uvicorn's access logging format
    LOGGING_CONFIG["formatters"]["access"]["fmt"] = '%(asctime)s [%(levelname)s] [%(filename)s] [%(process)d] %(client_addr)s - "%(request_line)s" %(status_code)s'
    LOGGING_CONFIG["formatters"]["access"]["datefmt"] = DATE_FMT
    
    # Modify uvicorn's default logging format
    LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(levelname)s] [%(filename)s] - %(message)s"
    LOGGING_CONFIG["formatters"]["default"]["datefmt"] = DATE_FMT

    # Create a new file handler for access logs and add it to uvicorn's logger
    file_handler_access = logging.FileHandler("access.log")
    file_handler_access.setFormatter(logging.Formatter(
        LOGGING_CONFIG["formatters"]["access"]["fmt"],
        DATE_FMT
    ))

    # Create a new file handler for default logs and add it to uvicorn's logger
    file_handler_default = logging.FileHandler("app.log")
    file_handler_default.setFormatter(logging.Formatter(
        LOGGING_CONFIG["formatters"]["default"]["fmt"],
        DATE_FMT
    ))

    # Add handlers to the LOGGING_CONFIG
    LOGGING_CONFIG["handlers"]["file_handler_access"] = {
        "class": "logging.FileHandler",
        "filename": "access.log",
        "formatter": "access"
    }
    LOGGING_CONFIG["handlers"]["file_handler_default"] = {
        "class": "logging.FileHandler",
        "filename": "app.log",
        "formatter": "default"
    }

    # Associate the handlers with the loggers
    LOGGING_CONFIG["loggers"]["uvicorn.access"]["handlers"].append("file_handler_access")
    LOGGING_CONFIG["loggers"]["uvicorn"]["handlers"].append("file_handler_default")

    print("logger setup")

    config = Config("main:app", host="0.0.0.0", log_config=LOGGING_CONFIG)
    server = uvicorn.Server(config=config)
    server.run()