import pandas as pd
import bt

from core.get_weigh import get_bdd_cap_weigh, get_cap_weigh

from data.get_data import get_pdf_df, get_prices_df

from core.get_backtest import get_eql_backtest, get_bdd_mkw_backtest, get_mkw_backtest, get_user_custom_backtest

from models import StrategyModel


def get_user_backtest(
    user_config: StrategyModel,
    upper_bound,
    start_date
    ):
    
    child_stk_tkr_list = [item["child_stk_tkr"] for item in user_config.myEtfPdf]
    child_prices = get_prices_df(tickers=child_stk_tkr_list, start_date=start_date)
    
    
    TICKER="AIQ"
    
    if user_config.rateMethod== "동일가중":
        
        # print(user_config.rateMethod)
        user_backtest = get_eql_backtest(
            name=user_config.myEtfName,
            child_prices=child_prices)
    
    elif user_config.rateMethod=="시가총액가중":
        
        # print(user_config.rateMethod)
        user_weigh = get_cap_weigh(
            child_prices=child_prices,
            pdf_df=get_pdf_df(etf_tkr=TICKER)
        )
        
        user_backtest = get_mkw_backtest(
            name=user_config.myEtfName,
            child_prices=child_prices,
            weigh=user_weigh
        )
        
    elif user_config.rateMethod=="ETF방식그대로":
        
        # print(user_config.rateMethod)
        user_weigh = get_bdd_cap_weigh(
            child_prices=child_prices,
            upper_bound=upper_bound,
            pdf_df=get_pdf_df(etf_tkr=TICKER)
        )
        
        user_backtest = get_bdd_mkw_backtest(
            name=user_config.myEtfName,
            child_prices=child_prices,
            weigh=user_weigh
        )
    
    else: # 수동
        
        # print(user_config.rateMethod)
        user_weigh = pd.DataFrame(user_config.myEtfPdf)
        
        user_backtest = get_user_custom_backtest(
            name=user_config.myEtfName,
            child_prices=child_prices,
            weigh=user_weigh
        )
        
    return user_backtest
    

def get_user_strategy(
    user_config: StrategyModel,
    start_date,
    upper_bound
    ):
    
    user_backtest = get_user_backtest(
        user_config=user_config,
        upper_bound=upper_bound,
        start_date=start_date
    )
    
    user_strategy = bt.run(user_backtest)
    
    return user_strategy
    

    
