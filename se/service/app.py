# from pinject import inject
#
# from se.domain2.time_series.time_series import HistoryDataQueryCommand, TimeSeriesRepo
#
#
# class TimeSeriesApp(object):
#
#     def download_data(self, ts_type_name: str, command: HistoryDataQueryCommand):
#         ts = self.repo.find_one(ts_type_name)
#         ts.download_data(command)
#
#     def history_data(self, ts_type_name: str, command: HistoryDataQueryCommand):
#         ts = self.repo.find_one(ts_type_name)
#         ts.history_data(command)
#
#     @inject()
#     def __init__(self, ts_repo: TimeSeriesRepo):
#         self.repo = ts_repo
