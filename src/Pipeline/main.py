import logging, beautifullogger
import sys
from Pipeline.pipeline import Pipeline
import pandas as pd
logger = logging.getLogger(__name__)

def setup_nice_logging():
    beautifullogger.setup(logmode="w")
    logging.getLogger("toolbox.ressource_manager").setLevel(logging.WARNING)
    logging.getLogger("toolbox.signal_analysis_toolbox").setLevel(logging.WARNING)

def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        logger.info("Keyboard interupt")
        sys.exit()
        return
    else:
        sys.__excepthook__(exc_type, exc_value, exc_traceback)


sys.excepthook = handle_exception


p = Pipeline()

@p.autocoord()
def subject():
    return pd.DataFrame([["s1", "toto"], ["s2", "titi"]], columns=["subject", "stupid"])

@p.autocoord()
def session(subject):
    if subject == "s1":
        return pd.DataFrame([["se1"], ["se2"]], columns=["session"])
    elif subject == "s2":
        return pd.DataFrame([["se3"]], columns=["session"])

def run():
    setup_nice_logging()
    logger.info("Running start")
    print(p.get_coords("session"))
    logger.info("Running end")