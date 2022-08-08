from yahooquery import Ticker
from tqdm import tqdm

def get_company_names(tickers:list[str])->tuple[list[str],list[str]]:
    short_company_names = []
    long_company_names = []
    infos = Ticker(tickers).quote_type
    for ticker in tqdm(tickers):
        info = infos[ticker]
        if info:
            if "shortName" in info:
                short_company_names.append(info["shortName"])
            else:
                short_company_names.append("")
            
            if "longName" in info:
                long_company_names.append(info["longName"])
            else:
                long_company_names.append("")
        else:
            short_company_names.append("")
            long_company_names.append("")
    return short_company_names,long_company_names