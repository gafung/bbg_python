from gafung_bbg import BbgService

stocks = ['1 HK Equity', '700 HK Equity']
fields = ['PX_LAST', 'PX_OPEN']
options = {
	'startDate': '20180301',
	'endDate': '20180310',
}
overrides = {}

bbg = BbgService()
result = bbg.get_data_historical(stocks, fields, options, overrides)
print('=' * 30)
print('Result:')
print(result)
print('=' * 30)

# Connecting to bloomberg host localhost:8194
# Sending Request: %s HistoricalDataRequest = {
#     securities[] = {
#         "1 HK Equity"
#     }
#     fields[] = {
#         "PX_LAST", "PX_OPEN"
#     }
#     endDate = "20180310"
#     startDate = "20180301"
#     overrides[] = {
#     }
# }

# Sending Request: %s HistoricalDataRequest = {
#     securities[] = {
#         "700 HK Equity"
#     }
#     fields[] = {
#         "PX_LAST", "PX_OPEN"
#     }
#     endDate = "20180310"
#     startDate = "20180301"
#     overrides[] = {
#     }
# }

# ==============================
# Result:
# {'700 HK Equity': [{'date': datetime.date(2018, 3, 1), 'PX_OPEN': 430.0, 'PX_LAST': 447.0}, {'date': datetime.date(2018, 3, 2), 'PX_OPEN': 437.0, 'PX_LAST': 436.0}, {'date': datetime.date(2018, 3, 5), 'PX_OPEN': 436.0, 'PX_LAST': 425.0}, {'date': datetime.date(2018, 3, 6), 'PX_OPEN': 435.0, 'PX_LAST': 438.6}, {'date': datetime.date(2018, 3, 7), 'PX_OPEN': 435.2, 'PX_LAST': 434.2}, {'date': datetime.date(2018, 3, 8), 'PX_OPEN': 440.0, 'PX_LAST': 443.0}, {'date': datetime.date(2018, 3, 9), 'PX_OPEN': 445.0, 'PX_LAST': 447.0}], '1 HK Equity': [{'date': datetime.date(2018, 3, 1), 'PX_OPEN': 96.55, 'PX_LAST': 98.05}, {'date': datetime.date(2018, 3, 2), 'PX_OPEN': 97.15, 'PX_LAST': 97.4}, {'date': datetime.date(2018, 3, 5), 'PX_OPEN': 97.1, 'PX_LAST': 95.2}, {'date': datetime.date(2018, 3, 6), 'PX_OPEN': 96.15, 'PX_LAST': 96.0}, {'date': datetime.date(2018, 3, 7), 'PX_OPEN': 96.0, 'PX_LAST': 95.3}, {'date': datetime.date(2018, 3, 8), 'PX_OPEN': 95.6, 'PX_LAST': 96.8}, {'date': datetime.date(2018, 3, 9), 'PX_OPEN': 97.5, 'PX_LAST': 98.75}]}
# ==============================
# Session stopped.