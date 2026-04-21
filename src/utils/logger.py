# +# import logging


# # def get_logger():
# #     logger = logging.getLogger("maven_logger")
# #     logger.setLevel(logging.INFO)

# #     if not logger.handlers:
# #         ch = logging.StreamHandler()
# #         logger.addHandler(ch)

# #     return logger


# import logging
# import json
# from datetime import datetime


# class StructuredLogger:
#     def __init__(self, pipeline_name="maven_market_pipeline"):
#         self.logger = logging.getLogger("maven_logger")
#         self.logger.setLevel(logging.INFO)

#         handler = logging.StreamHandler()
#         formatter = logging.Formatter('%(message)s')
#         handler.setFormatter(formatter)

#         if not self.logger.handlers:
#             self.logger.addHandler(handler)

#         self.pipeline_name = pipeline_name

#     def _log(self, level, message, **kwargs):
#         log = {
#             "timestamp": datetime.utcnow().isoformat(),
#             "pipeline": self.pipeline_name,
#             "level": level,
#             "message": message,
#             **kwargs
#         }
#         self.logger.log(level, json.dumps(log))

#     def info(self, message, **kwargs):
#         self._log(logging.INFO, message, **kwargs)

#     def error(self, message, **kwargs):
#         self._log(logging.ERROR, message, **kwargs)
        